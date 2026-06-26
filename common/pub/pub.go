package pub

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math" // 使用别名 mrand 来区分
	"math/rand"
	mrand "math/rand" // 设置别名 mrand
	"net/http"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"coffee_server/common/log"
	"coffee_server/config"

	"github.com/gin-gonic/gin"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

const charset_1 = "0123456789ABCDEFGHJKLMNPQRSTUWXYZ"

// generateRandomCode 生成一个指定长度的随机编码
func GenCode(n int) string {
	seededRand := rand.New(rand.NewSource(time.Now().UnixNano()))
	b := make([]byte, n)
	for i := range b {
		b[i] = charset_1[seededRand.Intn(len(charset_1))]
	}
	return string(b)
}

// 缓冲池用于复用bytes.Buffer，减少内存分配
var bufferPool = sync.Pool{
	New: func() interface{} {
		// 预分配1KB缓冲区，减少扩容次数
		buf := &bytes.Buffer{}
		buf.Grow(1024)
		return buf
	},
}

// 预定义的颜色代码常量，避免重复字符串分配
const (
	colorCyan    = "\033[36m"
	colorGreen   = "\033[32m"
	colorYellow  = "\033[33m"
	colorBlue    = "\033[34m"
	colorMagenta = "\033[35m"
	colorGray    = "\033[90m"
	colorReset   = "\033[0m"
)

// 预分配的缩进字符串缓存，避免重复生成
var indentCache = make([]string, 32) // 支持32层缩进

// 时间格式化缓存，避免重复的时间格式化操作
var timeCache struct {
	sync.Mutex
	lastTime time.Time
	timeStr  string
}

func init() {
	for i := 0; i < len(indentCache); i++ {
		indentCache[i] = strings.Repeat("  ", i)
	}
}

func T(a ...interface{}) {

}

// 获取格式化时间字符串，带缓存（1ms内复用）
func getFormattedTime() string {
	now := time.Now()
	timeCache.Lock()
	defer timeCache.Unlock()

	// 如果时间在1ms内，复用缓存的时间字符串
	if now.Sub(timeCache.lastTime) < time.Millisecond {
		return timeCache.timeStr
	}

	// 更新缓存
	timeCache.lastTime = now
	timeCache.timeStr = now.Format("15:04:05.000")
	return timeCache.timeStr
}

// 获取缩进字符串，带缓存
func getIndent(indent int) string {
	if indent < len(indentCache) {
		return indentCache[indent]
	}
	return strings.Repeat("  ", indent)
}

// D 高效的调试打印函数，支持结构化输出
// 只在调试模式下工作，生产环境中不执行任何操作
func D(args ...interface{}) {
	// 生产环境直接返回，零开销
	// if config.Mode != "debug" {
	// 	return
	// }

	// 从池中获取缓冲区
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)

	// 获取调用者信息
	_, file, line, ok := runtime.Caller(1)
	if !ok {
		file = "unknown"
		line = 0
	}

	// 只显示文件名，不显示完整路径
	if idx := strings.LastIndex(file, "/"); idx >= 0 {
		file = file[idx+1:]
	}

	// 构建调试信息头部 - 使用预定义常量减少字符串分配
	// buf.WriteString("\n")
	buf.WriteString(colorCyan)
	buf.WriteString("========== DEBUG ==========\n")

	// 预分配位置信息缓冲区，避免fmt.Sprintf分配
	buf.WriteString("📍 位置: ")
	buf.WriteString(file)
	buf.WriteString(":")
	buf.WriteString(strconv.Itoa(line))
	buf.WriteString("\n🕒 时间: ")
	buf.WriteString(getFormattedTime())
	buf.WriteString("\n📄 数据:\n")
	buf.WriteString(colorReset)

	// 处理每个参数
	for i, arg := range args {
		if i > 0 {
			buf.WriteString("\n")
		}
		formatValue(buf, arg, 0)
	}

	buf.WriteString(colorCyan)
	buf.WriteString("\n==========================")
	buf.WriteString(colorReset)
	buf.WriteString("\n")

	// 输出到控制台
	fmt.Print(buf.String())
}

// formatValue 格式化值并写入缓冲区
func formatValue(buf *bytes.Buffer, value interface{}, indent int) {
	if value == nil {
		buf.WriteString(colorGray)
		buf.WriteString("<nil>")
		buf.WriteString(colorReset)
		return
	}

	v := reflect.ValueOf(value)
	t := v.Type()

	// 处理指针
	if t.Kind() == reflect.Ptr {
		if v.IsNil() {
			buf.WriteString(colorGray)
			buf.WriteString("<nil>")
			buf.WriteString(colorReset)
			return
		}
		v = v.Elem()
		t = v.Type()
	}

	switch t.Kind() {
	case reflect.Map:
		formatMap(buf, v, indent)
	case reflect.Slice, reflect.Array:
		formatSlice(buf, v, indent)
	case reflect.Struct:
		formatStruct(buf, v, indent)
	case reflect.String:
		buf.WriteString(colorGreen)
		buf.WriteString("\"")
		buf.WriteString(v.String())
		buf.WriteString("\"")
		buf.WriteString(colorReset)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		buf.WriteString(colorYellow)
		buf.WriteString(strconv.FormatInt(v.Int(), 10))
		buf.WriteString(colorReset)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		buf.WriteString(colorYellow)
		buf.WriteString(strconv.FormatUint(v.Uint(), 10))
		buf.WriteString(colorReset)
	case reflect.Float32, reflect.Float64:
		buf.WriteString(colorYellow)
		// 使用更高效的浮点数格式化
		str := strconv.FormatFloat(v.Float(), 'g', 6, 64)
		buf.WriteString(str)
		buf.WriteString(colorReset)
	case reflect.Bool:
		buf.WriteString(colorMagenta)
		if v.Bool() {
			buf.WriteString("true")
		} else {
			buf.WriteString("false")
		}
		buf.WriteString(colorReset)
	default:
		// 尝试JSON序列化
		jsonBuf := bufferPool.Get().(*bytes.Buffer)
		jsonBuf.Reset()
		defer bufferPool.Put(jsonBuf)

		if jsonBytes, err := json.Marshal(value); err == nil {
			var prettyJSON bytes.Buffer
			if json.Indent(&prettyJSON, jsonBytes, getIndent(indent), "  ") == nil {
				buf.WriteString(prettyJSON.String())
				return
			}
		}
		// 如果JSON失败，使用默认格式
		buf.WriteString(fmt.Sprintf("%+v", value))
	}
}

// formatMap 格式化map类型
func formatMap(buf *bytes.Buffer, v reflect.Value, indent int) {
	if v.Len() == 0 {
		buf.WriteString("{}")
		return
	}

	buf.WriteString("{\n")
	keys := v.MapKeys()
	indentStr := getIndent(indent + 1)
	for i, key := range keys {
		// 缩进
		buf.WriteString(indentStr)

		// 键 - 使用更高效的字符串拼接
		buf.WriteString(colorBlue)
		switch key.Kind() {
		case reflect.String:
			buf.WriteString(key.String())
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			buf.WriteString(strconv.FormatInt(key.Int(), 10))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			buf.WriteString(strconv.FormatUint(key.Uint(), 10))
		default:
			buf.WriteString(fmt.Sprintf("%v", key.Interface()))
		}
		buf.WriteString(colorReset)
		buf.WriteString(": ")

		// 值
		mapValue := v.MapIndex(key)
		formatValue(buf, mapValue.Interface(), indent+1)

		if i < len(keys)-1 {
			buf.WriteString(",")
		}
		buf.WriteString("\n")
	}
	buf.WriteString(getIndent(indent))
	buf.WriteString("}")
}

// formatSlice 格式化切片/数组类型
func formatSlice(buf *bytes.Buffer, v reflect.Value, indent int) {
	length := v.Len()
	if length == 0 {
		buf.WriteString("[]")
		return
	}

	buf.WriteString("[\n")
	indentStr := getIndent(indent + 1)
	for i := 0; i < length; i++ {
		buf.WriteString(indentStr)
		formatValue(buf, v.Index(i).Interface(), indent+1)
		if i < length-1 {
			buf.WriteString(",")
		}
		buf.WriteString("\n")
	}
	buf.WriteString(getIndent(indent))
	buf.WriteString("]")
}

// formatStruct 格式化结构体类型
func formatStruct(buf *bytes.Buffer, v reflect.Value, indent int) {
	t := v.Type()
	numField := v.NumField()

	if numField == 0 {
		buf.WriteString("{}")
		return
	}

	buf.WriteString("{\n")
	indentStr := getIndent(indent + 1)
	for i := 0; i < numField; i++ {
		field := t.Field(i)
		fieldValue := v.Field(i)

		// 跳过未导出的字段
		if !fieldValue.CanInterface() {
			continue
		}

		// 缩进
		buf.WriteString(indentStr)

		// 字段名
		buf.WriteString(colorBlue)
		buf.WriteString(field.Name)
		buf.WriteString(colorReset)
		buf.WriteString(": ")

		// 字段值
		formatValue(buf, fieldValue.Interface(), indent+1)

		if i < numField-1 {
			buf.WriteString(",")
		}
		buf.WriteString("\n")
	}
	buf.WriteString(getIndent(indent))
	buf.WriteString("}")
}

// Dp 性能友好的调试输出，只在特定条件下输出
func Dp(condition bool, args ...interface{}) {
	if config.Mode != "debug" || !condition {
		return
	}
	D(args...)
}

// seededRand 是一个全局的伪随机数生成器实例。
// 我们在程序启动时只初始化一次，以获得更高的性能。
var seededRand = mrand.New(mrand.NewSource(time.Now().UnixNano()))

// GetRandStr 生成一个指定长度的随机字符串，包含大小写字母。
// 适用于生成文件名、密钥等。
const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func GetRandStr(n int) string {
	if n <= 0 {
		return ""
	}
	if n > 100 { // 设置合理的上限
		return ""
	}

	b := make([]byte, n)

	for i := range b {
		b[i] = letterBytes[seededRand.Intn(len(letterBytes))]
	}
	return string(b)
}

func R(c *gin.Context, res map[string]interface{}) {
	c.JSON(http.StatusOK, res)
	// c.Abort()
}

func Rd(c *gin.Context) {
	r := map[string]interface{}{
		"res": "done",
	}
	R(c, r)
}

func Re(c *gin.Context, errMsg string) {
	r := map[string]interface{}{
		"res":    "err",
		"errMsg": errMsg,
	}
	// D(errMsg)
	R(c, r)
	// c.JSON(http.StatusOK, r)
	// c.Abort()
}

// GetArg 从POST请求中获取JSON参数并转换为map。
// 参数：c *gin.Context
// 返回：map[string]interface{}, error
func GetArg(c *gin.Context) (map[string]interface{}, error) {
	arg := make(map[string]interface{})
	if err := c.BindJSON(&arg); err != nil {
		log.Error("解析JSON参数失败", zap.Error(err))
		return nil, fmt.Errorf("解析JSON参数失败: %w", err)
	}
	return arg, nil
}

//-------------------- 数据类型转换 --------------------

// ToDecimal 将多种类型的值转换为 decimal.Decimal。
// 如果转换失败，返回 decimal.Zero 和一个错误。
func InterToDecimal(val interface{}) (decimal.Decimal, error) {
	if val == nil {
		return decimal.Zero, nil
	}
	switch v := val.(type) {
	case decimal.Decimal:
		return v, nil
	case float64:
		return decimal.NewFromFloat(v), nil
	case float32:
		return decimal.NewFromFloat(float64(v)), nil
	case int, int64, int32:
		return decimal.NewFromInt(reflect.ValueOf(v).Int()), nil
	case string:
		return decimal.NewFromString(v)
	case json.Number:
		return decimal.NewFromString(v.String())
	case []byte:
		return decimal.NewFromString(string(v))
	default:
		return decimal.Zero, fmt.Errorf("不支持的类型转换: %T", val)
	}
}

// InterToInt 将 interface{} 转换为 int。
// 转换成功返回 int 值和 true，失败返回 0 和 false。
func InterToInt(i interface{}) (int, bool) {
	if i == nil {
		return 0, false
	}
	switch val := i.(type) {
	case int:
		return val, true
	case int64:
		return int(val), true
	case int32:
		return int(val), true
	case int16:
		return int(val), true
	case int8:
		return int(val), true
	case uint:
		return int(val), true
	case uint64:
		return int(val), true
	case uint32:
		return int(val), true
	case uint16:
		return int(val), true
	case uint8:
		return int(val), true
	case float64:
		if val == math.Trunc(val) && !math.IsNaN(val) && !math.IsInf(val, 0) {
			return int(val), true
		}
	case float32:
		val64 := float64(val)
		if val64 == math.Trunc(val64) && !math.IsNaN(val64) && !math.IsInf(val64, 0) {
			return int(val64), true
		}
	case string:
		if res, err := strconv.Atoi(val); err == nil {
			return res, true
		}
	case json.Number:
		if res, err := val.Int64(); err == nil {
			return int(res), true
		}
	}
	return 0, false
}

// InterToStr 高性能、鲁棒的任意类型转字符串
func InterToStr(v interface{}) string {
	if v == nil {
		return ""
	}

	// 高速路径：常见基础类型直接处理
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	case int:
		return strconv.Itoa(val)
	case int8:
		return strconv.FormatInt(int64(val), 10)
	case int16:
		return strconv.FormatInt(int64(val), 10)
	case int32:
		return strconv.FormatInt(int64(val), 10)
	case int64:
		return strconv.FormatInt(val, 10)
	case uint:
		return strconv.FormatUint(uint64(val), 10)
	case uint8:
		return strconv.FormatUint(uint64(val), 10)
	case uint16:
		return strconv.FormatUint(uint64(val), 10)
	case uint32:
		return strconv.FormatUint(uint64(val), 10)
	case uint64:
		return strconv.FormatUint(val, 10)
	case float32:
		return strconv.FormatFloat(float64(val), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	case bool:
		if val {
			return "true"
		}
		return "false"
	case error:
		return val.Error()
	case fmt.Stringer:
		return val.String()
	}

	// 慢速路径：复杂类型走 JSON
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)

	enc := json.NewEncoder(buf)
	enc.SetEscapeHTML(false) // 避免多余转义，性能略高
	if err := enc.Encode(v); err == nil {
		// 去掉 Encode 自动加的换行符
		res := buf.String()
		if len(res) > 0 && res[len(res)-1] == '\n' {
			res = res[:len(res)-1]
		}
		return res
	}

	// 兜底：fmt（理论上很少走到这里）
	return fmt.Sprintf("%v", v)
}

// InterToFloat 将 interface{} 转换为 float64。
// 转换成功返回 float64 值和 true，失败返回 0 和 false。
func InterToFloat(v interface{}) (float64, bool) {
	if v == nil {
		return 0, false
	}
	switch val := v.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	case int32:
		return float64(val), true
	case int16:
		return float64(val), true
	case int8:
		return float64(val), true
	case uint:
		return float64(val), true
	case uint64:
		return float64(val), true
	case uint32:
		return float64(val), true
	case uint16:
		return float64(val), true
	case uint8:
		return float64(val), true
	case string:
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f, true
		}
	case json.Number:
		if f, err := val.Float64(); err == nil {
			return f, true
		}
	}
	return 0, false
}

// NowStr 获取当前时间字符串，格式为 "2006-01-02 15:04:05.000000"。
func NowStr() string {
	return time.Now().Format("2006-01-02 15:04:05.000000")
}

// SetItem 重新设置map里的值，如果键不存在则添加。
func SetItem(m *map[string]interface{}, item string, value interface{}) {
	(*m)[item] = value
}

// DelArrItem 从切片中删除指定索引的元素。
// 参数：slice []interface{}, index int
// 返回：[]interface{}
func DelArrItem(slice []interface{}, index int) []interface{} {
	if index < 0 || index >= len(slice) {
		return slice
	}
	return append(slice[:index], slice[index+1:]...)
}

// IsEmpty 检查 interface{} 是否为空（优化版本）
// 这个版本使用类型断言优先于反射，提高了性能
// 同时增加了对更多类型的支持和递归检查的鲁棒性
func IsEmpty(i interface{}) bool {
	// 快速路径：nil 检查
	if i == nil {
		return true
	}

	// 使用类型断言处理常见类型，这比反射快得多
	switch v := i.(type) {
	case string:
		return v == ""
	case int:
		return v == 0
	case int8:
		return v == 0
	case int16:
		return v == 0
	case int32:
		return v == 0
	case int64:
		return v == 0
	case uint:
		return v == 0
	case uint8:
		return v == 0
	case uint16:
		return v == 0
	case uint32:
		return v == 0
	case uint64:
		return v == 0
	case float32:
		return v == 0
	case float64:
		return v == 0
	case bool:
		return !v
	case []interface{}:
		return len(v) == 0
	case []string:
		return len(v) == 0
	case []int:
		return len(v) == 0
	case map[string]interface{}:
		return len(v) == 0
	case time.Time:
		return v.IsZero()
	// 检查是否实现了自定义的 IsEmpty 方法
	case interface{ IsEmpty() bool }:
		return v.IsEmpty()
	}

	// 对于其他类型，使用反射（较慢但更通用）
	return isEmptyReflect(i)
}

// isEmptyReflect 使用反射检查值是否为空
// 这是 IsEmpty 的辅助函数，只用于不常见的类型
func isEmptyReflect(i interface{}) bool {
	v := reflect.ValueOf(i)

	// 处理无效的 reflect.Value
	if !v.IsValid() {
		return true
	}

	// 处理指针类型
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return true
		}
		// 解引用指针
		v = v.Elem()

		// 再次检查解引用后的值是否有效
		if !v.IsValid() {
			return true
		}
	}

	// 根据不同类型判断是否为空
	switch v.Kind() {
	case reflect.String:
		return v.String() == ""
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Map, reflect.Slice, reflect.Array:
		return v.Len() == 0
	case reflect.Struct:
		// 特殊处理 time.Time
		if v.Type().PkgPath() == "time" && v.Type().Name() == "Time" {
			return v.MethodByName("IsZero").Call(nil)[0].Bool()
		}

		// 对于一般结构体，检查所有字段是否为零值
		return isEmptyStruct(v, make(map[uintptr]bool))
	case reflect.Interface:
		return v.IsNil()
	case reflect.Chan, reflect.Func, reflect.UnsafePointer:
		return v.IsNil()
	case reflect.Complex64, reflect.Complex128:
		return v.Complex() == 0
	}

	return false
}

// isEmptyStruct 检查结构体是否为空，支持递归检查以避免循环引用
// visited 参数用于跟踪已访问的结构体，防止无限递归
func isEmptyStruct(v reflect.Value, visited map[uintptr]bool) bool {
	// 获取结构体的指针地址
	ptr := v.UnsafeAddr()

	// 检查是否已经访问过这个结构体（循环引用）
	if visited[ptr] {
		// 如果已经访问过，假设它是空的以避免无限递归
		return true
	}

	// 标记为已访问
	visited[ptr] = true
	defer func() { delete(visited, ptr) }()

	// 检查所有字段
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)

		// 跳过未导出的字段
		if !field.CanInterface() {
			continue
		}

		// 递归检查字段是否为空
		if !isEmptyValueReflect(field, visited) {
			return false
		}
	}

	return true
}

// isEmptyValueReflect 检查 reflect.Value 是否为零值
// 这是 isEmptyValue 的反射版本，支持递归检查
func isEmptyValueReflect(v reflect.Value, visited map[uintptr]bool) bool {
	if !v.IsValid() {
		return true
	}

	switch v.Kind() {
	case reflect.String:
		return v.String() == ""
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Map, reflect.Slice, reflect.Array:
		return v.Len() == 0
	case reflect.Ptr, reflect.Interface:
		if v.IsNil() {
			return true
		}

		// 解引用指针
		if v.Kind() == reflect.Ptr {
			v = v.Elem()
			if !v.IsValid() {
				return true
			}
		}

		// 处理结构体的递归检查
		if v.Kind() == reflect.Struct {
			return isEmptyStruct(v, visited)
		}

		return false
	case reflect.Struct:
		// 特殊处理 time.Time
		if v.Type().PkgPath() == "time" && v.Type().Name() == "Time" {
			return v.MethodByName("IsZero").Call(nil)[0].Bool()
		}

		return isEmptyStruct(v, visited)
	case reflect.Chan, reflect.Func, reflect.UnsafePointer:
		return v.IsNil()
	case reflect.Complex64, reflect.Complex128:
		return v.Complex() == 0
	}

	return false
}

// StructToMap 通用函数：将任何结构体转换为 map[string]interface{}
func StructToMap(obj interface{}) (map[string]interface{}, error) {
	// 1. 结构体 -> []byte (JSON 格式)
	data, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}

	// 2. []byte (JSON 格式) -> map[string]interface{}
	m := make(map[string]interface{})
	err = json.Unmarshal(data, &m)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// **通用结构体验证器**
// 接收一个结构体实例或结构体指针
func CheckStruct(s interface{}) error {
	v := reflect.ValueOf(s)
	if v.Kind() == reflect.Ptr {
		v = v.Elem() // 取消指针引用
	}

	if v.Kind() != reflect.Struct {
		return errors.New("input must be a struct or a struct pointer")
	}

	t := v.Type()

	// 遍历结构体的所有字段
	for i := 0; i < v.NumField(); i++ {
		field := t.Field(i)
		fieldValue := v.Field(i)

		// 1. 检查是否存在 "required" 校验 Tag
		if field.Tag.Get("req") == "true" {
			// 2. 检查字段值是否为空
			// 我们只对 string 类型进行 pub.IsEmpty 检查
			if fieldValue.Kind() == reflect.String {
				strVal := fieldValue.String()
				// 使用你的 pub.IsEmpty 函数进行校验
				if IsEmpty(strVal) {
					// 3. 获取错误信息 Tag
					errMsg := field.Tag.Get("msg")
					if errMsg == "" {
						errMsg = "字段校验失败: " + field.Name
					}
					D(errMsg)
					return errors.New(errMsg)
				}
			}
			// 如果需要校验 int, bool, array 等非空，需要更复杂的逻辑
		}
	}

	return nil
}

// MapToStruct 使用 JSON 编解码将 map[string]interface{} 转换为结构体。
//
// source: 原始的 map[string]interface{} 数据。
// target: 目标结构体的指针，例如 &types.ImageItem{}.
//
// 返回值：错误（如果发生转换失败）。
func MapToStruct(source map[string]interface{}, target interface{}) error {
	// 1. 检查 target 是否是结构体指针
	targetValue := reflect.ValueOf(target)
	if targetValue.Kind() != reflect.Ptr || targetValue.IsNil() {
		return errors.New("MapToStruct: target must be a non-nil pointer to a struct")
	}
	if targetValue.Elem().Kind() != reflect.Struct {
		return errors.New("MapToStruct: target must be a pointer to a struct")
	}

	// 2. Map -> []byte (JSON 格式)
	// 这一步是安全的，map[string]interface{} 总是可以被 JSON 序列化
	data, err := json.Marshal(source)
	if err != nil {
		return errors.New("MapToStruct Marshal error: " + err.Error())
	}

	// 3. []byte (JSON 格式) -> Struct (目标结构体)
	// JSON 反序列化负责处理字段映射和类型转换
	err = json.Unmarshal(data, target)
	if err != nil {
		// 如果源 Map 中的值与目标 Struct 的类型不匹配，会在这里报错
		return errors.New("MapToStruct Unmarshal error (type mismatch or bad format): " + err.Error())
	}

	return nil
}

func ToMap(value interface{}) interface{} {
	switch v := value.(type) {
	case map[interface{}]interface{}:
		newMap := make(map[string]interface{})
		for k, val := range v {
			newMap[fmt.Sprintf("%v", k)] = ToMap(val)
		}
		return newMap
	case []interface{}:
		for i, val := range v {
			v[i] = ToMap(val)
		}
		return v
	case map[string]interface{}:
		for k, val := range v {
			v[k] = ToMap(val)
		}
		return v
	default:
		return v
	}
}

func NoApi(c *gin.Context, api ...interface{}) { // 修改为变长参数
	errMsg := "没有找到API"

	// 获取调用者的信息 (skip 1 依然指向调用 NoApi 的位置)
	pc, file, line, ok := runtime.Caller(1)
	if ok {
		fn := runtime.FuncForPC(pc).Name()

		// 处理可选的 api 参数
		apiStr := "未提供"
		if len(api) > 0 {
			apiStr = InterToStr(api[0]) // 取第一个参数
		}

		// 格式化追踪信息
		traceInfo := fmt.Sprintf("没有找到API\n[追踪]\n 文件: %s \n 行号: %d \n 函数: %s\n API: %s", file, line, fn, apiStr)

		D(traceInfo)
	}

	Re(c, errMsg)
}

func ToDecimal(val interface{}) decimal.Decimal {
	switch v := val.(type) {
	case nil:
		return decimal.Zero
	case decimal.Decimal:
		return v
	case float64:
		return decimal.NewFromFloat(v)
	case float32:
		return decimal.NewFromFloat(float64(v))
	case int:
		return decimal.NewFromInt(int64(v))
	case int64:
		return decimal.NewFromInt(v)
	case int32:
		return decimal.NewFromInt(int64(v))
	case string:
		d, err := decimal.NewFromString(v)
		if err != nil {
			return decimal.Zero
		}
		return d
	default:
		s := ""
		switch vv := v.(type) {
		case []byte:
			s = string(vv)
		default:
			s = fmt.Sprintf("%v", vv)
		}
		d, err := decimal.NewFromString(s)
		if err != nil {
			return decimal.Zero
		}
		return d
	}
}

func GenBillId(prefix string) string {
	// 1. 获取当前时间 (年月日时分秒)
	now := time.Now().Format("20060102150405")

	// 2. 生成 4 位随机数 (防止同一秒内产生重复)
	// 记得在 main 或 init 中初始化 rand.Seed
	randNum := rand.Intn(9000) + 1000

	return fmt.Sprintf("%s%s%d", prefix, now, randNum)
}
