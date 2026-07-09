// Package config
package config

import "github.com/rs/zerolog"

type Config struct {
	Database    *Database
	Application *Application
}

type Database struct {
	Host     string `koanf:"host" validate:"required"`
	Port     int    `koanf:"port" validate:"required"`
	User     string `koanf:"user" validate:"required"`
	Password string `koanf:"password" validate:"required"`
	Name     string `koanf:"name" validate:"required"`

	SSLMode         string `koanf:"ssl_mode" validate:"required"`
	MaxOpenConns    int    `koanf:"max_open_conns" validate:"required"`
	MaxIdleConns    int    `koanf:"max_idle_conns" validate:"required"`
	ConnMaxLifetime int    `koanf:"conn_max_lifetime" validate:"required"`
	ConnMaxIdleTime int    `koanf:"conn_max_idle_time" validate:"required"`
}

type Application struct {
	Environment    *string `koanf:"environment"`
	MigrationsPath string  `koanf:"path" validate:"required"`
}

func LoadConfig(logger *zerolog.Logger) (*Config, error) {
	return &Config{
		Database: &Database{
			Host:     "localhost",
			Port:     5432,
			User:     "postgres",
			Password: "password",
			Name:     "agentflow",
		},
		Application: &Application{
			MigrationsPath: "file:///home/surgee/Desktop/core/agentflow/migrations",
			Environment:    nil,
		},
	}, nil
}
