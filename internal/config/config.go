// Package config loads runtime configuration from a .env file and the process
// environment.
//
// Precedence, lowest to highest: built-in defaults, then the .env file, then
// real environment variables. That ordering is what lets a developer keep a
// .env for everyday use while a deployment overrides individual values without
// shipping a file, and it lets a test override one value without restating the
// rest.
package config

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"

	validator "github.com/go-playground/validator/v10"
	"github.com/knadh/koanf/parsers/dotenv"
	"github.com/knadh/koanf/providers/env/v2"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/providers/structs"
	"github.com/knadh/koanf/v2"
	"github.com/rs/zerolog"
)

const (
	// envPrefix namespaces the variables this process reads, so a var named
	// DB_USER for the Postgres container cannot be mistaken for application
	// config.
	envPrefix = "AGENTFLOW__"

	// sectionDelim separates config sections in a variable name. Two
	// underscores rather than one because key names contain single underscores
	// of their own: AGENTFLOW__DATABASE__MAX_OPEN_CONNS has to split into
	// database and max_open_conns, which a single underscore cannot express.
	sectionDelim = "__"

	// keyDelim is the delimiter in the flattened key path koanf builds.
	keyDelim = "."

	// defaultEnvFile is loaded when present and skipped when absent, since a
	// deployment supplies real environment variables instead.
	defaultEnvFile = ".env"
)

type Config struct {
	Database   *Database   `koanf:"database"`
	Migrations *Migrations `koanf:"migrations"`
	Engine     *Engine     `koanf:"engine"`
}

type Database struct {
	Host     string `koanf:"host" validate:"required"`
	Port     int    `koanf:"port" validate:"required"`
	User     string `koanf:"user" validate:"required"`
	Password string `koanf:"password" validate:"required"`
	Name     string `koanf:"name" validate:"required"`

	// SSLMode is passed through to the DSN verbatim, so it has to be a value
	// libpq recognises rather than a boolean.
	SSLMode         string `koanf:"ssl_mode" validate:"required,oneof=disable allow prefer require verify-ca verify-full"`
	MaxOpenConns    int    `koanf:"max_open_conns" validate:"required,gt=0"`
	MaxIdleConns    int    `koanf:"max_idle_conns" validate:"gte=0"`
	ConnMaxLifetime int    `koanf:"conn_max_lifetime" validate:"required,gt=0"`
	ConnMaxIdleTime int    `koanf:"conn_max_idle_time" validate:"required,gt=0"`
}

// Engine configures a single node. These are node properties rather than
// workflow properties: capacity is a function of this host's CPU and memory,
// not of what any manifest asked for.
type Engine struct {
	// Capacity is the number of tasks this node will run concurrently. The
	// dispatcher never claims past it — a lease you cannot honour is worse than
	// a task left unclaimed.
	Capacity int `koanf:"capacity" validate:"required,gt=0"`

	// HeartbeatInterval is how often the registrar refreshes heartbeat_at, in
	// seconds. Liveness is tracked per engine rather than per task, so this is
	// one write per node per interval regardless of how much work is in flight.
	HeartbeatInterval int `koanf:"heartbeat_interval" validate:"required,gt=0"`

	// LeaseTTL is how long a claim is honoured for, in seconds. It has to be a
	// comfortable multiple of HeartbeatInterval: the reaper treats an engine as
	// dead once its heartbeat is older than this, so a TTL close to the
	// interval reclaims work from nodes that are merely busy.
	LeaseTTL int `koanf:"lease_ttl" validate:"required,gt=0,gtfield=HeartbeatInterval"`
}

type Migrations struct {
	// MigrationsPath is a golang-migrate source URL, not a filesystem path —
	// it is handed straight to migrate.NewWithDatabaseInstance.
	MigrationsPath string `koanf:"path" validate:"required,startswith=file://"`
}

// defaults are the values used when neither the .env file nor the environment
// supplies one. They target local development against docker-compose.dev.yml;
// anything genuinely environment-specific is left to fail validation rather
// than given a default that would silently work in the wrong place.
func defaults() Config {
	return Config{
		Database: &Database{
			Host:     "localhost",
			Port:     5433,
			User:     "postgres",
			Password: "password",
			// The database scripts/init_db.sql creates, which is where the
			// uuid-ossp/pg_trgm extensions and the update_updated_at trigger
			// function live. The container's POSTGRES_DB (agentflow_db) is a
			// separate, empty database — connecting to that one migrates into a
			// schema with no extensions.
			Name:            "agentflow",
			SSLMode:         "disable",
			MaxOpenConns:    10,
			MaxIdleConns:    2,
			ConnMaxLifetime: 3600,
			ConnMaxIdleTime: 300,
		},
		Migrations: &Migrations{
			// Relative to the working directory, so the repo runs from a clone
			// without anyone editing a path.
			MigrationsPath: "file://migrations",
		},
		Engine: &Engine{
			Capacity:          4,
			HeartbeatInterval: 5,
			// Twelve heartbeats of slack before a node is declared dead.
			LeaseTTL: 60,
		},
	}
}

// Options controls where configuration is read from. The zero value loads
// ".env" and the real process environment, which is what main wants; tests set
// the fields to read from a temporary file and a fixed variable list instead.
type Options struct {
	// EnvFile is the dotenv file to load. Empty means defaultEnvFile. A file
	// that does not exist is skipped, but one that exists and cannot be parsed
	// is an error — a typo should not silently fall back to defaults.
	EnvFile string

	// Environ supplies environment variables as "KEY=value" strings. Nil means
	// os.Environ.
	Environ func() []string
}

// LoadConfig reads configuration from defaults, the .env file and the
// environment, then validates it.
func LoadConfig(logger *zerolog.Logger) (*Config, error) {
	return LoadConfigWithOptions(logger, Options{})
}

func LoadConfigWithOptions(logger *zerolog.Logger, opts Options) (*Config, error) {
	envFile := opts.EnvFile
	if envFile == "" {
		envFile = defaultEnvFile
	}

	k := koanf.New(keyDelim)

	// Defaults are loaded through the structs provider so they share the koanf
	// tags with everything layered on top; there is no second spelling of a key
	// to drift out of sync.
	base := defaults()
	if err := k.Load(structs.Provider(base, "koanf"), nil); err != nil {
		logger.Error().Err(err).Str("func", "LoadConfig").Msg("failed to load config defaults")
		return nil, fmt.Errorf("load config defaults: %w", err)
	}

	if err := loadEnvFile(k, logger, envFile); err != nil {
		return nil, err
	}

	// Real environment variables load last so they win over the file.
	envProvider := env.Provider(keyDelim, env.Opt{
		Prefix:        envPrefix,
		TransformFunc: transform,
		EnvironFunc:   opts.Environ,
	})
	if err := k.Load(envProvider, nil); err != nil {
		logger.Error().Err(err).Str("func", "LoadConfig").Msg("failed to load environment variables")
		return nil, fmt.Errorf("load environment variables: %w", err)
	}

	var cfg Config
	if err := k.UnmarshalWithConf("", &cfg, koanf.UnmarshalConf{Tag: "koanf"}); err != nil {
		logger.Error().Err(err).Str("func", "LoadConfig").Msg("failed to unmarshal config")
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}

	if err := validator.New().Struct(cfg); err != nil {
		logger.Error().Err(err).Str("func", "LoadConfig").Msg("invalid configuration")
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	logger.Info().
		Str("func", "LoadConfig").
		Str("host", cfg.Database.Host).
		Int("port", cfg.Database.Port).
		Str("database", cfg.Database.Name).
		Str("ssl_mode", cfg.Database.SSLMode).
		Msg("configuration loaded")

	return &cfg, nil
}

// loadEnvFile layers a dotenv file over whatever is already in k. A missing
// file is not an error: deployments pass real environment variables and ship no
// file at all.
func loadEnvFile(k *koanf.Koanf, logger *zerolog.Logger, envFile string) error {
	if _, err := os.Stat(envFile); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			logger.Debug().
				Str("func", "LoadConfig").
				Str("file", envFile).
				Msg("no env file found, using defaults and environment")
			return nil
		}
		logger.Error().Err(err).Str("func", "LoadConfig").Str("file", envFile).Msg("failed to stat env file")
		return fmt.Errorf("stat env file %s: %w", envFile, err)
	}

	parser := dotenv.ParserEnvWithValue(envPrefix, keyDelim, func(k, v string) (string, any) {
		return transform(k, v)
	})
	if err := k.Load(file.Provider(envFile), parser); err != nil {
		logger.Error().Err(err).Str("func", "LoadConfig").Str("file", envFile).Msg("failed to load env file")
		return fmt.Errorf("load env file %s: %w", envFile, err)
	}

	logger.Debug().Str("func", "LoadConfig").Str("file", envFile).Msg("env file loaded")
	return nil
}

// transform maps an environment variable name onto a koanf key path:
//
//	AGENTFLOW__DATABASE__MAX_OPEN_CONNS -> database.max_open_conns
//
// Variables outside the prefix return an empty key, which both providers treat
// as "ignore this one".
func transform(key, value string) (string, any) {
	trimmed, found := strings.CutPrefix(key, envPrefix)
	if !found || trimmed == "" {
		return "", nil
	}

	sections := strings.Split(trimmed, sectionDelim)
	for i, section := range sections {
		// An empty section means a malformed name such as a trailing or tripled
		// delimiter. Dropping it would silently bind the value to the wrong key,
		// so the variable is ignored instead.
		if section == "" {
			return "", nil
		}
		sections[i] = strings.ToLower(section)
	}

	return strings.Join(sections, keyDelim), value
}
