package db

import (
	"coffee_server/common/pub"
	"coffee_server/config"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

// 全局常量，用于配置数据库连接池
const (
	maxOpenConns    = 100             // 最大打开连接数
	maxIdleConns    = 20              // 最大空闲连接数
	connMaxLifetime = time.Hour       // 连接可重用的最长时间
	queryTimeout    = 5 * time.Second // 查询操作的默认超时时间
	execTimeout     = 3 * time.Second // 写入操作的默认超时时间
	maxRetries      = 3               // 数据库操作失败后的最大重试次数
)

// 全局变量，用于实现数据库连接池单例
var (
	instance *DB       // 数据库单例
	once     sync.Once // 确保单例只初始化一次
)

// DB 封装了 sqlx.DB，提供了更高级的数据库操作，现在为私有
type DB struct {
	*sqlx.DB
	debug   bool       // 调试模式标志，控制日志输出
	stats   *DBStats   // 数据库操作统计
	metrics *DBMetrics // 性能指标
}

// DBStats 数据库统计信息，使用原子操作保证并发安全。
type DBStats struct {
	QueryCount     atomic.Int64 // 查询操作次数统计
	ExecCount      atomic.Int64 // 执行操作次数统计
	ErrorCount     atomic.Int64 // 错误次数统计
	SlowQueryCount atomic.Int64 // 慢查询次数统计（超过1秒的查询）
}

// DBMetrics 性能指标，使用原子操作记录操作耗时。
type DBMetrics struct {
	QueryDuration atomic.Int64 // 最新一次查询的耗时（纳秒）
	ExecDuration  atomic.Int64 // 最新一次执行的耗时（纳秒）
	MaxQueryTime  atomic.Int64 // 最长一次查询的耗时（毫秒）
}

// newDB 采用单例模式创建并返回一个优化的数据库连接实例。
// 此函数为私有，仅供本包内的公共函数调用。
func newDB() *DB {
	once.Do(func() {
		cfg := getConfig()
		if err := validateConfig(&cfg); nil != err {
			panic(fmt.Sprintf("数据库配置无效: %v", err))
		}

		dsn := buildDSN(cfg)
		db, err := sqlx.Connect("mysql", dsn)
		if nil != err {
			panic(fmt.Sprintf("连接数据库失败: %v", err))
		}

		// 优化连接池设置
		db.SetMaxOpenConns(maxOpenConns)
		db.SetMaxIdleConns(maxIdleConns)
		db.SetConnMaxLifetime(connMaxLifetime)

		instance = &DB{
			DB:      db,
			debug:   config.Mode == "debug",
			stats:   &DBStats{},
			metrics: &DBMetrics{},
		}

		// 预热连接池
		warmupPool(instance.DB)

		// 启动监控和维护任务
		// go instance.startMonitoring()
		go instance.startMaintenance()
	})
	return instance
}

// Query 是包级别的公共函数，执行带有占位符(?)的查询操作。
func Query(ctx context.Context, query string, args ...interface{}) ([]map[string]interface{}, error) {
	// 内部调用私有的 newDB() 获取单例，然后执行查询
	dbInstance := newDB()
	// 为本次查询设置超时上下文
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	start := time.Now()
	defer func() {
		dbInstance.recordMetrics("query", start)
	}()

	rows, err := dbInstance.QueryxContext(ctx, query, args...)
	if nil != err {
		dbInstance.logError("查询执行失败", err, query, args)
		return nil, fmt.Errorf("查询执行失败: %w", err)
	}
	defer rows.Close()

	return dbInstance.scanRows(rows)
}

// 错误分析函数
func analyzeError(err error, query string, args interface{}) string {
	if err == nil {
		return ""
	}

	errStr := err.Error()

	switch {
	case strings.Contains(errStr, "panic") && args == nil:
		return fmt.Sprintf("❌ 致命错误: 传入了nil参数导致panic\n"+
			"🔧 解决方案: 传入空map而不是nil，如: map[string]interface{}{}\n"+
			"📍 出错位置: %s", query)
	case strings.Contains(errStr, "invalid memory address") || strings.Contains(errStr, "nil pointer"):
		return fmt.Sprintf("❌ 空指针错误: 参数为nil\n"+
			"🔧 解决方案: 检查传入的参数，确保不为nil\n"+
			"📍 查询: %s\n"+
			"📍 参数: %v", query, args)
	case strings.Contains(errStr, "syntax error"):
		return fmt.Sprintf("❌ SQL语法错误\n"+
			"🔧 解决方案: 检查SQL语句语法\n"+
			"📍 语句: %s", query)
	case strings.Contains(errStr, "no such table"):
		return fmt.Sprintf("❌ 表不存在\n"+
			"🔧 解决方案: 检查表名是否正确，确保数据库中存在该表\n"+
			"📍 语句: %s", query)
	case strings.Contains(errStr, "no such column"):
		return fmt.Sprintf("❌ 字段不存在\n"+
			"🔧 解决方案: 检查字段名是否正确\n"+
			"📍 语句: %s", query)
	case strings.Contains(errStr, "duplicate"):
		return fmt.Sprintf("❌ 违反唯一约束 - 数据重复\n"+
			"🔧 解决方案: 检查插入的数据是否已存在\n"+
			"📍 错误: %s", errStr)
	case strings.Contains(errStr, "connection"):
		return fmt.Sprintf("❌ 数据库连接错误\n"+
			"🔧 解决方案: 检查数据库服务是否正常，网络是否通畅\n"+
			"📍 错误: %s", errStr)
	case strings.Contains(errStr, "timeout"):
		return fmt.Sprintf("❌ 查询超时\n"+
			"🔧 解决方案: 优化SQL语句，添加索引或增加超时时间\n"+
			"📍 语句: %s", query)
	case strings.Contains(errStr, "bind"):
		return fmt.Sprintf("❌ 参数绑定失败\n"+
			"🔧 解决方案: 检查参数类型和占位符是否匹配\n"+
			"📍 查询: %s\n"+
			"📍 参数: %v", query, args)
	default:
		return fmt.Sprintf("❌ 未知数据库错误\n"+
			"📍 错误: %s\n"+
			"📍 SQL: %s\n"+
			"📍 参数: %v", errStr, query, args)
	}
}

// Exec 是包级别的公共函数，执行带有占位符(?)的写操作。
func Exec(ctx context.Context, query string, args ...interface{}) (int64, error) {
	dbInstance := newDB()
	// 为本次操作设置超时上下文
	ctx, cancel := context.WithTimeout(ctx, execTimeout)
	defer cancel()

	start := time.Now()
	defer func() {
		dbInstance.recordMetrics("exec", start)
	}()

	var affected int64
	err := dbInstance.retry(ctx, maxRetries, func() error {
		result, err := dbInstance.execWithArgs(ctx, query, args...)
		if nil != err {
			return err
		}
		affected, err = result.RowsAffected()
		return err
	})

	if nil != err {
		dbInstance.logError("执行操作失败", err, query, args)
		return 0, fmt.Errorf("执行操作失败: %w", err)
	}

	return affected, nil
}

// ExecResult 表示写操作结果：最后插入ID与受影响行数
type ExecResult struct {
	LastInsertId int64 `json:"lastInsertId"`
	Affected     int64 `json:"affected"`
}

// mergeParamMaps 将多个 map 合并，后面的 key 会覆盖前面的 key
func mergeParamMaps(maps ...map[string]interface{}) map[string]interface{} {
	merged := make(map[string]interface{})
	for _, m := range maps {
		if m == nil {
			continue
		}
		for k, v := range m {
			merged[k] = v
		}
	}
	return merged
}

// GetStruct 支持传入一个或多个结构体作为命名参数。
// 内部会将结构体转换为 map[string]interface{}，然后调用 Get。
func GetStruct(ctx context.Context, query string, params ...interface{}) ([]map[string]interface{}, error) {
	maps := make([]map[string]interface{}, 0, len(params))
	for i, param := range params {
		paramMap, err := pub.StructToMap(param)
		if err != nil {
			return nil, fmt.Errorf("参数 %d 转换为 map 失败: %w", i, err)
		}
		maps = append(maps, paramMap)
	}
	// 最终调用现有的 Get 函数
	return Get(ctx, query, maps...)
}

// DoStruct 支持传入一个或多个结构体作为命名参数。
// 内部会将结构体转换为 map[string]interface{}，然后调用 Do。
func DoStruct(ctx context.Context, query string, params ...interface{}) (*ExecResult, error) {
	maps := make([]map[string]interface{}, 0, len(params))
	for i, param := range params {
		paramMap, err := pub.StructToMap(param)
		if err != nil {
			return nil, fmt.Errorf("参数 %d 转换为 map 失败: %w", i, err)
		}
		maps = append(maps, paramMap)
	}
	// 最终调用现有的 Do 函数
	return Do(ctx, query, maps...)
}

// Get 支持多个命名参数 map，合并后使用 NamedQueryContext。
// 调用示例： Get(ctx, "select ... where a=:a and b=:b", map[string]interface{}{"a":1}, map[string]interface{}{"b":"x"})
func Get(ctx context.Context, query string, params ...map[string]interface{}) ([]map[string]interface{}, error) {
	dbInstance := newDB()
	// 设置超时上下文
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	// 合并命名参数
	args := mergeParamMaps(params...)
	if args == nil {
		args = map[string]interface{}{}
	}

	start := time.Now()
	defer func() {
		dbInstance.recordMetrics("query", start)
	}()

	// 捕获 sqlx 可能的 panic（如传入 nil 导致 panic）
	var rows *sqlx.Rows
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("数据库操作发生panic: %v", r)
				if dbInstance.debug {
					dbInstance.logError("数据库操作panic", err, query, args)
					dbInstance.logError("Panic详情", fmt.Errorf("可能原因: 1.参数为nil 2.SQL语法错误 3.表或字段不存在"), "", nil)
				}
			}
		}()
		rows, err = dbInstance.NamedQueryContext(ctx, query, args)
	}()

	if err != nil {
		errorDetail := analyzeError(err, query, args)
		dbInstance.logError("命名查询执行失败", errors.New(errorDetail), query, args)
		return nil, fmt.Errorf("命名查询执行失败: %w", err)
	}
	defer rows.Close()

	result, err := dbInstance.scanRows(rows)
	if err != nil {
		dbInstance.logError("扫描结果失败", err, query, args)
		return nil, err
	}

	// 记录成功的查询日志（仅 debug）
	// duration := time.Since(start)
	// dbInstance.logQuery(query, args, duration)

	return result, nil
}

// Do 支持多个命名参数 map，合并后使用 NamedExecContext，返回 ExecResult。
// 调用示例： res, err := Do(ctx, "insert ... values(:a,:b)", map[string]interface{}{"a":1}, map[string]interface{}{"b":"x"})
func Do(ctx context.Context, query string, params ...map[string]interface{}) (*ExecResult, error) {
	dbInstance := newDB()
	// 设置超时上下文
	ctx, cancel := context.WithTimeout(ctx, execTimeout)
	defer cancel()

	// 合并命名参数
	args := mergeParamMaps(params...)
	if args == nil {
		args = map[string]interface{}{}
	}

	start := time.Now()
	defer func() {
		dbInstance.recordMetrics("exec", start)
	}()

	var lastInsertId int64
	var affected int64

	err := dbInstance.retry(ctx, maxRetries, func() error {
		result, err := dbInstance.NamedExecContext(ctx, query, args)
		if err != nil {
			return err
		}
		if id, e := result.LastInsertId(); e == nil {
			lastInsertId = id
		}
		if aff, e := result.RowsAffected(); e == nil {
			affected = aff
		}
		return nil
	})

	if err != nil {
		dbInstance.logError("执行命名操作失败", err, query, args)
		return nil, fmt.Errorf("执行命名操作失败: %w", err)
	}

	// duration := time.Since(start)
	// dbInstance.logQuery(query, args, duration)

	return &ExecResult{
		LastInsertId: lastInsertId,
		Affected:     affected,
	}, nil
}

// TxGet 在给定事务 (tx) 中执行命名参数查询，并返回结果。
// 必须在 db.Tx 或 db.Tran 的回调函数内部调用。
func TxGet(tx *sqlx.Tx, ctx context.Context, query string, params ...map[string]interface{}) ([]map[string]interface{}, error) {
	dbInstance := newDB()

	// 为本次查询设置超时上下文
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	// 1. 合并命名参数
	argsMap := mergeParamMaps(params...)
	if argsMap == nil {
		argsMap = map[string]interface{}{}
	}

	start := time.Now()
	defer func() {
		dbInstance.recordMetrics("query", start)
	}()

	var rows *sqlx.Rows
	var err error

	// --- 核心修复部分 ---
	// 2. 使用 sqlx.BindNamed 将命名参数转换为标准 SQL (query, args)
	// 捕获 sqlx.BindNamed 可能的 panic (例如 args 传入非法类型)
	var boundQuery string
	var boundArgs []interface{}
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("sqlx.BindNamed 发生panic: %v", r)
				if dbInstance.debug {
					dbInstance.logError("BindNamed panic", err, query, argsMap)
				}
			}
		}()
		// 将命名查询和参数 map 绑定，生成标准的 '?' 占位符 SQL
		boundQuery, boundArgs, err = tx.BindNamed(query, argsMap)
	}()

	// 如果绑定失败，提前返回错误
	if err != nil {
		errorDetail := analyzeError(err, query, argsMap)
		dbInstance.logError("参数绑定失败", errors.New(errorDetail), query, argsMap)
		return nil, fmt.Errorf("事务命名查询参数绑定失败: %w", err)
	}

	// 3. 在事务上使用标准的 QueryxContext 方法
	rows, err = tx.QueryxContext(ctx, boundQuery, boundArgs...)
	// --- 核心修复结束 ---

	// 如果有错误
	if err != nil {
		errorDetail := analyzeError(err, query, argsMap)
		dbInstance.logError("事务命名查询执行失败", errors.New(errorDetail), query, argsMap)
		return nil, fmt.Errorf("事务命名查询执行失败: %w", err)
	}
	defer rows.Close()

	result, err := dbInstance.scanRows(rows)
	if err != nil {
		dbInstance.logError("事务扫描结果失败", err, query, argsMap)
		return nil, err
	}

	return result, nil
}

// TxDo 在给定事务 (tx) 中执行命名参数写操作，并返回 ExecResult。
// 必须在 db.Tx 或 db.Tran 的回调函数内部调用。
func TxDo(tx *sqlx.Tx, ctx context.Context, query string, params ...map[string]interface{}) (*ExecResult, error) {
	// 获取 dbInstance 仅用于访问其 debug 标志、统计和日志方法，实际数据库操作使用传入的 tx
	dbInstance := newDB()

	// 为本次操作设置超时上下文
	ctx, cancel := context.WithTimeout(ctx, execTimeout)
	defer cancel()

	// 合并命名参数
	args := mergeParamMaps(params...)
	if args == nil {
		args = map[string]interface{}{}
	}

	start := time.Now()
	defer func() {
		dbInstance.recordMetrics("exec", start)
	}()

	var lastInsertId int64
	var affected int64

	// 在事务上执行写操作，并支持重试
	err := dbInstance.retry(ctx, maxRetries, func() error {
		result, err := tx.NamedExecContext(ctx, query, args) // 在事务上执行命名写操作
		if err != nil {
			return err
		}
		if id, e := result.LastInsertId(); e == nil {
			lastInsertId = id
		}
		if aff, e := result.RowsAffected(); e == nil {
			affected = aff
		}
		return nil
	})

	if err != nil {
		dbInstance.logError("执行事务命名操作失败", err, query, args)
		return nil, fmt.Errorf("执行事务命名操作失败: %w", err)
	}

	// duration := time.Since(start)
	// dbInstance.logQuery(query, args, duration)

	return &ExecResult{
		LastInsertId: lastInsertId,
		Affected:     affected,
	}, nil
}

// Transaction 事务处理。
func Tran(fn func(*sqlx.Tx) error) error {
	ctx := context.Background()
	dbInstance := newDB()
	return dbInstance.transaction(ctx, fn)
}

func Tx(ctx context.Context, fn func(*sqlx.Tx) error) error {
	dbInstance := newDB()
	return dbInstance.transaction(ctx, fn)
}

// transaction 事务处理，私有方法。
func (db *DB) transaction(ctx context.Context, fn func(*sqlx.Tx) error) error {
	if db.debug {
		log.Printf("\033[33m[SQL] 开始事务\033[0m")
	}
	tx, err := db.BeginTxx(ctx, nil)
	if nil != err {
		return fmt.Errorf("开启事务失败: %w", err)
	}

	// 使用 recover 捕获 panic，确保事务能够回滚
	defer func() {
		if p := recover(); nil != p {
			_ = tx.Rollback()
			db.logError("事务panic回滚", fmt.Errorf("%v", p), "", nil)
			panic(p)
		}
	}()
	if err := fn(tx); nil != err {
		if rbErr := tx.Rollback(); nil != rbErr {
			db.logError("事务回滚失败", rbErr, "", nil)
			return fmt.Errorf("事务回滚失败: %v (原始错误: %w)", rbErr, err)
		}
		return err
	}
	if err := tx.Commit(); nil != err {
		return fmt.Errorf("事务提交失败: %w", err)
	}
	if db.debug {
		log.Printf("\033[32m[SQL] 事务提交成功\033[0m")
	}
	return nil
}

// Close 优雅地关闭数据库连接池。
func Close() {
	dbInstance := newDB()
	dbInstance.DB.Close()
}

// GetStats 获取数据库统计信息。
func GetStats() *DBStats {
	dbInstance := newDB()
	return dbInstance.stats
}

// 下面的所有辅助函数保持私有，与之前版本相同，无需修改
func (db *DB) logQuery(query string, args interface{}, duration time.Duration) {
	if !db.debug {
		return
	}
	_, file, line, _ := runtime.Caller(3)
	parts := strings.Split(file, "/")
	if len(parts) > 2 {
		file = strings.Join(parts[len(parts)-2:], "/")
	}
	argsStr := formatArgs(args)

	displayQuery := replaceNamedParams(query, args)

	logMsg := fmt.Sprintf("[SQL] [位置: %s:%d]\n语句: %s\n参数: %s\n耗时: %v",
		file, line, displayQuery, argsStr, duration)
	if duration > time.Second {
		log.Printf("\033[33m%s [慢查询警告]\033[0m\n", logMsg)
	} else {
		log.Printf("\033[34m%s\033[0m\n", logMsg)
	}
}

func (db *DB) logError(msg string, err error, query string, args interface{}) {
	if !db.debug {
		return
	}
	_, file, line, _ := runtime.Caller(3)
	logMsg := fmt.Sprintf("[SQL错误] [位置: %s:%d]\n消息: %s\n错误: %v",
		file, line, msg, err)
	if query != "" {
		// 替换命名参数为实际值（仅在debug模式下执行）
		displayQuery := replaceNamedParams(query, args)
		logMsg += fmt.Sprintf("\n语句: %s\n参数: %v", displayQuery, formatArgs(args))
	}
	log.Printf("\033[31m%s\033[0m\n", logMsg)
	db.stats.ErrorCount.Add(1)
}

func (db *DB) retry(ctx context.Context, maxRetries int, fn func() error) error {
	var err error
	for i := 0; i < maxRetries; i++ {
		// 核心改动：在每次重试前检查上下文是否已取消
		select {
		case <-ctx.Done():
			// 如果上下文已取消，立即停止重试并返回上下文的错误
			return ctx.Err()
		default:
			// 继续执行
		}

		if err = fn(); err == nil {
			return nil
		}

		// 如果错误不可重试，则立即返回
		if !db.isRetryableError(err) {
			return err
		}

		// 打印重试日志（调试模式下）
		if db.debug {
			log.Printf("\033[33m[SQL] 第%d次重试, 错误: %v\033[0m\n", i+1, err)
		}

		// 指数退避，等待一段时间再重试
		time.Sleep(time.Duration(i+1) * 100 * time.Millisecond)
	}

	// 达到最大重试次数后，返回最后一次的错误
	return err
}

func (db *DB) isRetryableError(err error) bool {
	if nil == err {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "deadlock") ||
		strings.Contains(errStr, "connection reset") ||
		strings.Contains(errStr, "too many connections")
}

func formatArgs(args interface{}) string {
	switch v := args.(type) {
	case map[string]interface{}:
		pairs := make([]string, 0, len(v))
		for key, val := range v {
			pairs = append(pairs, fmt.Sprintf("%s=%v", key, val))
		}
		return strings.Join(pairs, ", ")
	default:
		return fmt.Sprintf("%v", args)
	}
}

func (db *DB) startMonitoring() {
	ticker := time.NewTicker(time.Minute)
	go func() {
		for range ticker.C {
			stats := db.DB.Stats()
			log.Printf("\033[36m[数据库监控]\n"+
				"打开连接数: %d\n"+
				"使用中连接: %d\n"+
				"空闲连接数: %d\n"+
				"等待数: %d\n"+
				"慢查询数: %d\n"+
				"错误数: %d\033[0m\n",
				stats.OpenConnections,
				stats.InUse,
				stats.Idle,
				stats.WaitCount,
				db.stats.SlowQueryCount.Load(),
				db.stats.ErrorCount.Load(),
			)
		}
	}()
}

func (db *DB) startMaintenance() {
	healthTicker := time.NewTicker(5 * time.Minute)
	go func() {
		for range healthTicker.C {
			db.checkPoolHealth()
		}
	}()
}

func (db *DB) checkPoolHealth() {
	stats := db.Stats()
	if float64(stats.InUse)/float64(stats.MaxOpenConnections) > 0.8 {
		if db.debug {
			log.Printf("\033[33m[警告] 连接池使用率超过80%%: %d/%d\033[0m\n",
				stats.InUse, stats.MaxOpenConnections)
		}
	}
	if stats.WaitCount > 100 && stats.WaitDuration.Seconds() > 1 {
		if db.debug {
			log.Printf("\033[33m[警告] 连接等待次数过多: %d, 平均等待时间: %v\033[0m\n",
				stats.WaitCount, stats.WaitDuration)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := db.PingContext(ctx); nil != err {
		if db.debug {
			log.Printf("\033[31m[错误] 数据库心跳检测失败: %v\033[0m\n", err)
		}
	}
}

type Config struct {
	Username string
	Password string
	Host     string
	Port     int
	Database string
	Charset  string
}

// getConfig 获取数据库配置。
func getConfig() Config {
	if config.Mode == "release" {
		return Config{
			Username: "root",
			Password: "xufei727",
			Host:     "localhost",
			Port:     3306,
			Database: "tc",
			Charset:  "utf8mb4",
		}
	}
	return Config{
		Username: "root",
		Password: "xufei727",
		Host:     "localhost",
		Port:     3306,
		Database: "tcdebug",
		Charset:  "utf8mb4",
	}
}

func validateConfig(cfg *Config) error {
	if cfg.Username == "" {
		return fmt.Errorf("数据库用户名不能为空")
	}
	if cfg.Host == "" {
		return fmt.Errorf("数据库主机地址不能为空")
	}
	if cfg.Port <= 0 || cfg.Port > 65535 {
		return fmt.Errorf("无效的数据库端口: %d", cfg.Port)
	}
	if cfg.Database == "" {
		return fmt.Errorf("数据库名称不能为空")
	}
	return nil
}

func buildDSN(cfg Config) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s&parseTime=true&loc=Local",
		cfg.Username, cfg.Password, cfg.Host, cfg.Port, cfg.Database, cfg.Charset)
}

func warmupPool(db *sqlx.DB) {
	for i := 0; i < maxIdleConns; i++ {
		conn, err := db.Conn(context.Background())
		if nil == err {
			go func() {
				defer conn.Close()
				_ = conn.PingContext(context.Background())
			}()
		}
	}
}

func (db *DB) execWithArgs(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	if len(args) > 0 {
		return db.ExecContext(ctx, query, args...)
	}
	return db.ExecContext(ctx, query)
}

func (db *DB) scanRows(rows *sqlx.Rows) ([]map[string]interface{}, error) {
	var result []map[string]interface{}
	for rows.Next() {
		row := make(map[string]interface{})
		if err := rows.MapScan(row); nil != err {
			return nil, fmt.Errorf("扫描数据失败: %w", err)
		}
		for k, v := range row {
			if b, ok := v.([]byte); ok {
				row[k] = string(b)
			}
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

func (db *DB) recordMetrics(operation string, start time.Time) {
	duration := time.Since(start)
	// db.logQuery(operation, nil, duration)
	switch operation {
	case "query":
		db.stats.QueryCount.Add(1)
		db.metrics.QueryDuration.Store(int64(duration))
		if duration > time.Second {
			db.stats.SlowQueryCount.Add(1)
		}
	case "exec":
		db.stats.ExecCount.Add(1)
		db.metrics.ExecDuration.Store(int64(duration))
	}
	currentMax := db.metrics.MaxQueryTime.Load()
	if duration.Milliseconds() > currentMax {
		db.metrics.MaxQueryTime.Store(duration.Milliseconds())
	}
}

func replaceNamedParams(query string, args interface{}) string {
	// 检查是否为debug模式
	if config.Mode != "debug" {
		return query
	}

	if args == nil {
		return query
	}

	switch v := args.(type) {
	case map[string]interface{}:
		result := query
		for key, value := range v {
			placeholder := ":" + key
			// 将参数值转换为字符串
			valueStr := fmt.Sprintf("%v", value)
			// 替换占位符
			result = strings.ReplaceAll(result, placeholder, valueStr)
		}
		return result
	default:
		// 对于非map类型，直接返回原查询
		return query
	}
}
