package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

func testLogger() *zerolog.Logger {
	logger := zerolog.Nop()
	return &logger
}

// writeEnvFile puts a dotenv file in a temp dir and returns its path.
func writeEnvFile(t *testing.T, contents string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("failed to write env file: %v", err)
	}
	return path
}

// environ builds an EnvironFunc from "KEY=value" strings, so a test controls the
// entire environment the loader sees rather than inheriting the real one.
func environ(vars ...string) func() []string {
	return func() []string { return vars }
}

// load runs the loader against a fully controlled file and environment. An
// empty envFile points at a path that does not exist, which exercises the
// no-file path rather than accidentally picking up the repo's own .env.
func load(t *testing.T, envFile string, vars ...string) (*Config, error) {
	t.Helper()

	if envFile == "" {
		envFile = filepath.Join(t.TempDir(), "absent.env")
	}
	return LoadConfigWithOptions(testLogger(), Options{
		EnvFile: envFile,
		Environ: environ(vars...),
	})
}

func TestLoadConfig_Defaults(t *testing.T) {
	cfg, err := load(t, "")
	if err != nil {
		t.Fatalf("expected defaults to load, got: %v", err)
	}

	want := defaults()
	if *cfg.Database != *want.Database {
		t.Errorf("Database = %+v, want %+v", *cfg.Database, *want.Database)
	}
	if *cfg.Migrations != *want.Migrations {
		t.Errorf("Migrations = %+v, want %+v", *cfg.Migrations, *want.Migrations)
	}
}

// The defaults have to satisfy the same validation rules as anything supplied
// externally, or the zero-config path fails only at startup.
func TestDefaults_AreValid(t *testing.T) {
	if _, err := load(t, ""); err != nil {
		t.Fatalf("built-in defaults must pass validation, got: %v", err)
	}
}

// A .env file that does not exist is a normal deployment shape, not an error.
func TestLoadConfig_MissingEnvFileIsNotAnError(t *testing.T) {
	cfg, err := load(t, filepath.Join(t.TempDir(), "nope.env"))
	if err != nil {
		t.Fatalf("a missing env file should be skipped, got: %v", err)
	}
	if cfg.Database.Host != "localhost" {
		t.Errorf("Host = %q, want the default", cfg.Database.Host)
	}
}

func TestLoadConfig_FromEnvFile(t *testing.T) {
	path := writeEnvFile(t, `
AGENTFLOW__DATABASE__HOST=db.internal
AGENTFLOW__DATABASE__PORT=6000
AGENTFLOW__DATABASE__USER=agentflow
AGENTFLOW__DATABASE__PASSWORD=s3cret
AGENTFLOW__DATABASE__NAME=production
AGENTFLOW__DATABASE__SSL_MODE=require
AGENTFLOW__DATABASE__MAX_OPEN_CONNS=50
AGENTFLOW__MIGRATIONS__PATH=file:///srv/migrations
`)

	cfg, err := load(t, path)
	if err != nil {
		t.Fatalf("expected env file to load, got: %v", err)
	}

	if cfg.Database.Host != "db.internal" {
		t.Errorf("Host = %q, want db.internal", cfg.Database.Host)
	}
	// Values arrive as strings and must coerce to the field's type.
	if cfg.Database.Port != 6000 {
		t.Errorf("Port = %d, want 6000", cfg.Database.Port)
	}
	if cfg.Database.User != "agentflow" {
		t.Errorf("User = %q, want agentflow", cfg.Database.User)
	}
	if cfg.Database.Password != "s3cret" {
		t.Errorf("Password = %q, want s3cret", cfg.Database.Password)
	}
	if cfg.Database.Name != "production" {
		t.Errorf("Name = %q, want production", cfg.Database.Name)
	}
	if cfg.Database.SSLMode != "require" {
		t.Errorf("SSLMode = %q, want require", cfg.Database.SSLMode)
	}
	// The double underscore has to split section from key without eating the
	// single underscores inside the key name.
	if cfg.Database.MaxOpenConns != 50 {
		t.Errorf("MaxOpenConns = %d, want 50", cfg.Database.MaxOpenConns)
	}
	if cfg.Migrations.MigrationsPath != "file:///srv/migrations" {
		t.Errorf("MigrationsPath = %q, want file:///srv/migrations", cfg.Migrations.MigrationsPath)
	}
}

// Keys absent from the file keep their defaults rather than being zeroed.
func TestLoadConfig_EnvFileIsPartial(t *testing.T) {
	path := writeEnvFile(t, "AGENTFLOW__DATABASE__HOST=only-host\n")

	cfg, err := load(t, path)
	if err != nil {
		t.Fatalf("expected a partial env file to load, got: %v", err)
	}

	if cfg.Database.Host != "only-host" {
		t.Errorf("Host = %q, want only-host", cfg.Database.Host)
	}
	if cfg.Database.Port != defaults().Database.Port {
		t.Errorf("Port = %d, want the default %d", cfg.Database.Port, defaults().Database.Port)
	}
	if cfg.Database.SSLMode != defaults().Database.SSLMode {
		t.Errorf("SSLMode = %q, want the default", cfg.Database.SSLMode)
	}
}

func TestLoadConfig_FromEnvironment(t *testing.T) {
	cfg, err := load(t, "",
		"AGENTFLOW__DATABASE__HOST=from-env",
		"AGENTFLOW__DATABASE__PORT=7000",
		"AGENTFLOW__DATABASE__CONN_MAX_IDLE_TIME=90",
	)
	if err != nil {
		t.Fatalf("expected environment to load, got: %v", err)
	}

	if cfg.Database.Host != "from-env" {
		t.Errorf("Host = %q, want from-env", cfg.Database.Host)
	}
	if cfg.Database.Port != 7000 {
		t.Errorf("Port = %d, want 7000", cfg.Database.Port)
	}
	if cfg.Database.ConnMaxIdleTime != 90 {
		t.Errorf("ConnMaxIdleTime = %d, want 90", cfg.Database.ConnMaxIdleTime)
	}
}

// The precedence rule the whole design rests on: a deployment overrides one
// value without shipping or editing a file.
func TestLoadConfig_EnvironmentOverridesEnvFile(t *testing.T) {
	path := writeEnvFile(t, `
AGENTFLOW__DATABASE__HOST=from-file
AGENTFLOW__DATABASE__PORT=6000
AGENTFLOW__DATABASE__NAME=from-file-db
`)

	cfg, err := load(t, path,
		"AGENTFLOW__DATABASE__HOST=from-env",
		"AGENTFLOW__DATABASE__PORT=7000",
	)
	if err != nil {
		t.Fatalf("expected config to load, got: %v", err)
	}

	if cfg.Database.Host != "from-env" {
		t.Errorf("Host = %q, want the environment to win", cfg.Database.Host)
	}
	if cfg.Database.Port != 7000 {
		t.Errorf("Port = %d, want the environment to win", cfg.Database.Port)
	}
	// Untouched by the environment, so the file still supplies it.
	if cfg.Database.Name != "from-file-db" {
		t.Errorf("Name = %q, want the file value to survive", cfg.Database.Name)
	}
	// Touched by neither, so the default survives both layers.
	if cfg.Database.User != defaults().Database.User {
		t.Errorf("User = %q, want the default to survive", cfg.Database.User)
	}
}

// Variables outside the prefix belong to something else — the Postgres
// container's own DB_USER is the case that actually occurs.
func TestLoadConfig_IgnoresUnprefixedVariables(t *testing.T) {
	path := writeEnvFile(t, `
DB_USER=container-user
DB_NAME=container-db
DB_PASSWORD=container-password
AGENTFLOW__DATABASE__HOST=app-host
`)

	cfg, err := load(t, path, "DB_USER=another-container-user", "HOST=bare-host")
	if err != nil {
		t.Fatalf("expected config to load, got: %v", err)
	}

	if cfg.Database.User != defaults().Database.User {
		t.Errorf("User = %q, want DB_USER to be ignored", cfg.Database.User)
	}
	if cfg.Database.Name != defaults().Database.Name {
		t.Errorf("Name = %q, want DB_NAME to be ignored", cfg.Database.Name)
	}
	if cfg.Database.Host != "app-host" {
		t.Errorf("Host = %q, want app-host", cfg.Database.Host)
	}
}

func TestLoadConfig_MalformedEnvFile(t *testing.T) {
	// An unterminated quote is a parse error rather than a line godotenv skips.
	path := writeEnvFile(t, "AGENTFLOW__DATABASE__HOST=\"unterminated\nAGENTFLOW__DATABASE__PORT=1\n")

	if _, err := load(t, path); err == nil {
		t.Fatal("expected a malformed env file to fail rather than silently fall back to defaults")
	}
}

func TestLoadConfig_ValidationFailures(t *testing.T) {
	tests := []struct {
		name    string
		vars    []string
		wantErr string
	}{
		{
			name:    "ssl mode given as a boolean",
			vars:    []string{"AGENTFLOW__DATABASE__SSL_MODE=false"},
			wantErr: "SSLMode",
		},
		{
			name:    "unrecognised ssl mode",
			vars:    []string{"AGENTFLOW__DATABASE__SSL_MODE=maybe"},
			wantErr: "SSLMode",
		},
		{
			name:    "empty host",
			vars:    []string{"AGENTFLOW__DATABASE__HOST="},
			wantErr: "Host",
		},
		{
			name:    "port zero",
			vars:    []string{"AGENTFLOW__DATABASE__PORT=0"},
			wantErr: "Port",
		},
		{
			name:    "empty password",
			vars:    []string{"AGENTFLOW__DATABASE__PASSWORD="},
			wantErr: "Password",
		},
		{
			name:    "pool size of zero",
			vars:    []string{"AGENTFLOW__DATABASE__MAX_OPEN_CONNS=0"},
			wantErr: "MaxOpenConns",
		},
		{
			// A plain filesystem path is the mistake this catches: golang-migrate
			// needs a source URL and reports an unhelpful error without one.
			name:    "migrations path missing the file scheme",
			vars:    []string{"AGENTFLOW__MIGRATIONS__PATH=/srv/migrations"},
			wantErr: "MigrationsPath",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg, err := load(t, "", test.vars...)
			if err == nil {
				t.Fatalf("expected a validation error mentioning %q, got config %+v", test.wantErr, cfg)
			}
			if cfg != nil {
				t.Errorf("expected nil config on error, got %+v", cfg)
			}
			if !strings.Contains(err.Error(), test.wantErr) {
				t.Errorf("error = %q, want it to name %s", err, test.wantErr)
			}
		})
	}
}

// A non-numeric value for an int field must fail at load rather than coerce to
// zero and then trip the validator with a misleading message.
func TestLoadConfig_NonNumericPort(t *testing.T) {
	_, err := load(t, "", "AGENTFLOW__DATABASE__PORT=not-a-number")
	if err == nil {
		t.Fatal("expected a non-numeric port to fail")
	}
}

func TestTransform(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		wantKey string
	}{
		{"section and key", "AGENTFLOW__DATABASE__HOST", "database.host"},
		{"underscores inside the key survive", "AGENTFLOW__DATABASE__MAX_OPEN_CONNS", "database.max_open_conns"},
		{"single section", "AGENTFLOW__MIGRATIONS__PATH", "migrations.path"},
		{"already lowercase", "AGENTFLOW__database__host", "database.host"},

		// Ignored: an empty key tells both providers to drop the variable.
		{"no prefix", "DATABASE__HOST", ""},
		{"prefix only", "AGENTFLOW__", ""},
		{"partial prefix", "AGENTFLOW_DATABASE__HOST", ""},
		{"lowercase prefix is not matched", "agentflow__database__host", ""},
		{"trailing delimiter", "AGENTFLOW__DATABASE__", ""},
		{"tripled delimiter", "AGENTFLOW__DATABASE____HOST", ""},
		{"leading delimiter", "AGENTFLOW____HOST", ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gotKey, gotValue := transform(test.key, "value")
			if gotKey != test.wantKey {
				t.Errorf("transform(%q) key = %q, want %q", test.key, gotKey, test.wantKey)
			}
			if test.wantKey == "" {
				return
			}
			if gotValue != "value" {
				t.Errorf("transform(%q) value = %v, want it passed through", test.key, gotValue)
			}
		})
	}
}

// Malformed names are dropped rather than bound to a mangled key, so a typo
// leaves the default in place instead of writing to the wrong field.
func TestLoadConfig_MalformedKeysAreIgnored(t *testing.T) {
	cfg, err := load(t, "",
		"AGENTFLOW__DATABASE____HOST=mangled",
		"AGENTFLOW__DATABASE__=mangled",
	)
	if err != nil {
		t.Fatalf("expected malformed keys to be ignored, got: %v", err)
	}

	if cfg.Database.Host != defaults().Database.Host {
		t.Errorf("Host = %q, want the default", cfg.Database.Host)
	}
}

// LoadConfig reads the real process environment. Covered separately from the
// injected path because that wiring is what main depends on.
func TestLoadConfig_ReadsProcessEnvironment(t *testing.T) {
	t.Setenv("AGENTFLOW__DATABASE__HOST", "real-env-host")
	t.Chdir(t.TempDir()) // no .env here, so defaults plus the real environment

	cfg, err := LoadConfig(testLogger())
	if err != nil {
		t.Fatalf("expected config to load, got: %v", err)
	}
	if cfg.Database.Host != "real-env-host" {
		t.Errorf("Host = %q, want real-env-host", cfg.Database.Host)
	}
}

// The committed sample must stay loadable and consistent with the defaults, so
// copying it to .env is never the thing that breaks a fresh clone.
func TestEnvSampleIsValid(t *testing.T) {
	path := filepath.Join("..", "..", ".env.sample")
	if _, err := os.Stat(path); err != nil {
		t.Skipf("no .env.sample to check: %v", err)
	}

	cfg, err := LoadConfigWithOptions(testLogger(), Options{
		EnvFile: path,
		Environ: environ(),
	})
	if err != nil {
		t.Fatalf(".env.sample must load cleanly, got: %v", err)
	}

	want := defaults()
	if *cfg.Database != *want.Database {
		t.Errorf(".env.sample has drifted from the defaults:\n got %+v\nwant %+v",
			*cfg.Database, *want.Database)
	}
	if *cfg.Migrations != *want.Migrations {
		t.Errorf(".env.sample migrations drifted:\n got %+v\nwant %+v",
			*cfg.Migrations, *want.Migrations)
	}
}
