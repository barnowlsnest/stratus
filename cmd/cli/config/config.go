package config

import (
	"github.com/barnowlsnest/go-configlib/v2/pkg/configs"
)

type Config struct {
	LogLevel string `name:"loglevel" default:"info" usage:"log level for the cli commands"`
	Host     string `name:"host" default:"localhost" usage:"stratus server host"`
	Port     int    `name:"port" default:"8000" usage:"stratus server port"`
}

func Load() (*Config, error) {
	var cfg Config
	_, err := configs.Resolve(&cfg, "")
	if err != nil {
		return nil, err
	}

	return &cfg, nil
}
