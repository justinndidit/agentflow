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
	"net"
	"net/url"
	"os"
	"strconv"
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
	Blob       *Blob       `koanf:"blob"`
	Telemetry  *Telemetry  `koanf:"telemetry"`
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

	// PollInterval is the floor, in seconds, on how often a node looks for work
	// when no notification wakes it sooner. Notifications are the fast path;
	// this only covers one that was missed or never delivered.
	PollInterval int `koanf:"poll_interval" validate:"required,gt=0"`

	// ReapInterval is how often, in seconds, a node sweeps for abandoned work.
	// Deliberately coarse relative to the heartbeat: reaping early is worse
	// than reaping late, because it duplicates work that is still running.
	ReapInterval int `koanf:"reap_interval" validate:"required,gt=0"`

	// Runtime selects how tasks are executed. "docker" runs the agent's
	// container image; "echo" runs no container and returns the input as the
	// output, which is what the scheduling tests exercise.
	Runtime string `koanf:"runtime" validate:"required,oneof=docker echo"`

	// TaskMemoryMB caps a single container's memory. A node runs several at
	// once, so the ceiling that matters is the host's rather than one task's.
	TaskMemoryMB int64 `koanf:"task_memory_mb" validate:"required,gt=0"`

	// TaskCPUs caps a single container's CPU, in whole cores. Fractions are
	// allowed: 0.5 is half a core.
	TaskCPUs float64 `koanf:"task_cpus" validate:"required,gt=0"`

	// TaskNetwork is the Docker network mode for a task container. "none"
	// isolates it completely; agents that call a model provider need "bridge".
	TaskNetwork string `koanf:"task_network" validate:"required"`
}

// Blob addresses S3-compatible storage for task outputs too large for
// Postgres. It is optional: with Enabled false, workers get no artifact
// destination and are expected to return their output inline.
type Blob struct {
	Enabled bool `koanf:"enabled"`

	// Endpoint is host:port. Empty means AWS S3 itself; a local MinIO is
	// typically localhost:9000.
	Endpoint string `koanf:"endpoint"`

	AccessKey string `koanf:"access_key"`
	SecretKey string `koanf:"secret_key"`
	Bucket    string `koanf:"bucket"`
	Region    string `koanf:"region"`

	// UseSSL should only be false for a local MinIO on plain HTTP.
	UseSSL bool `koanf:"use_ssl"`
}

// Validate checks the fields that only matter once storage is switched on.
// They cannot carry `required` tags, because an engine with no blob storage is
// a legitimate configuration and would otherwise fail to start.
func (b *Blob) Validate() error {
	if b == nil || !b.Enabled {
		return nil
	}
	if b.Bucket == "" {
		return errors.New("blob storage is enabled but no bucket is set")
	}
	if b.AccessKey == "" || b.SecretKey == "" {
		return errors.New("blob storage is enabled but credentials are missing")
	}
	return nil
}

// Telemetry addresses an OpenTelemetry collector. Optional: with it disabled
// every span and metric in the codebase becomes a no-op, so the instrumentation
// costs nothing and the call sites read the same either way.
type Telemetry struct {
	Enabled bool `koanf:"enabled"`

	// Endpoint is the collector's host:port for OTLP over gRPC.
	Endpoint string `koanf:"endpoint"`

	ServiceName string `koanf:"service_name"`

	// Insecure disables TLS to the collector. Appropriate for a collector on
	// the same host or inside the same network, and nowhere else.
	Insecure bool `koanf:"insecure"`

	// SampleRatio is the fraction of workflows traced, from 0 to 1.
	//
	// Sampling is per trace and a workflow is one trace, so a sampled workflow
	// is sampled whole. Half of every workflow's spans would be far less useful
	// than all of half of them.
	SampleRatio float64 `koanf:"sample_ratio" validate:"gte=0,lte=1"`
}

// Validate checks the fields that only matter once telemetry is switched on.
// They cannot carry `required` tags, because an engine with no collector is a
// legitimate configuration and would otherwise fail to start.
func (t *Telemetry) Validate() error {
	if t == nil || !t.Enabled {
		return nil
	}
	if t.Endpoint == "" {
		return errors.New("telemetry is enabled but no collector endpoint is set")
	}
	if t.ServiceName == "" {
		return errors.New("telemetry is enabled but no service name is set")
	}
	return nil
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
		Blob: &Blob{
			// Off by default: an engine with nowhere to put artifacts still
			// runs every workflow whose outputs fit inline, which is most of
			// them, and demanding storage credentials to start would make the
			// common case harder for no benefit.
			Enabled:  false,
			Endpoint: "localhost:9000",
			Bucket:   "agentflow-artifacts",
			Region:   "us-east-1",
			UseSSL:   false,
		},
		Telemetry: &Telemetry{
			// Off by default, for the same reason as blob storage: an engine
			// with no collector runs every workflow perfectly well, and
			// demanding one to start would make the common case harder.
			Enabled:     false,
			Endpoint:    "localhost:4317",
			ServiceName: "agentflow",
			Insecure:    true,
			SampleRatio: 1,
		},
		Engine: &Engine{
			Capacity:          4,
			HeartbeatInterval: 5,
			// Twelve heartbeats of slack before a node is declared dead.
			LeaseTTL:     60,
			PollInterval: 2,
			ReapInterval: 15,
			// Echo by default: the seeded development agents point at
			// placeholder images that are not published anywhere, so docker
			// would fail every task on a fresh checkout.
			Runtime:      "echo",
			TaskMemoryMB: 512,
			TaskCPUs:     1,
			TaskNetwork:  "bridge",
		},
	}
}

// DSN renders the connection string for this database.
//
// It lives on the config rather than in the database package because more than
// one consumer needs it: the pool routes transactional work through it, and the
// LISTEN connection is opened directly from it, deliberately bypassing any
// pooler. Transaction-mode pooling silently breaks LISTEN, so those two paths
// must not share a connection source.
func (d *Database) DSN() string {
	hostPort := net.JoinHostPort(d.Host, strconv.Itoa(d.Port))

	return fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=%s",
		url.QueryEscape(d.User),
		url.QueryEscape(d.Password),
		hostPort,
		d.Name,
		d.SSLMode,
	)
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

	if err := cfg.Blob.Validate(); err != nil {
		logger.Error().Err(err).Str("func", "LoadConfig").Msg("invalid blob configuration")
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	if err := cfg.Telemetry.Validate(); err != nil {
		logger.Error().Err(err).Str("func", "LoadConfig").Msg("invalid telemetry configuration")
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
