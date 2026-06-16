package config

import "time"

type Config struct {
	Addr         string
	Token        string
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration
}

func Load() Config {
	return Config{
		Addr:         Addr,
		Token:        Token,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  70 * time.Second,
	}
}
