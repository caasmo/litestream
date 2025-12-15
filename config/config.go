package config

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"time"
	"os/user"
	"path/filepath"

	"github.com/dustin/go-humanize"
	"gopkg.in/yaml.v2"

	"github.com/benbjohnson/litestream"

)

// Sentinel errors for configuration validation
var (
	ErrInvalidSnapshotInterval         = errors.New("snapshot interval must be greater than 0")
	ErrInvalidSnapshotRetention        = errors.New("snapshot retention must be greater than 0")
	ErrInvalidCompactionInterval       = errors.New("compaction interval must be greater than 0")
	ErrInvalidSyncInterval             = errors.New("sync interval must be greater than 0")
	ErrInvalidL0Retention              = errors.New("l0 retention must be greater than 0")
	ErrInvalidL0RetentionCheckInterval = errors.New("l0 retention check interval must be greater than 0")
	ErrConfigFileNotFound              = errors.New("config file not found")
)

// ConfigValidationError wraps a validation error with additional context
type ConfigValidationError struct {
	Err   error
	Field string
	Value interface{}
}

func (e *ConfigValidationError) Error() string {
	if e.Value != nil {
		return fmt.Sprintf("%s: %v (got %v)", e.Field, e.Err, e.Value)
	}
	return fmt.Sprintf("%s: %v", e.Field, e.Err)
}

func (e *ConfigValidationError) Unwrap() error {
	return e.Err
}

// Config represents a configuration file for the litestream daemon.
type Config struct {
	// Global replica settings that serve as defaults for all replicas
	ReplicaSettings `yaml:",inline"`

	// Bind address for serving metrics.
	Addr string `yaml:"addr"`

	// List of stages in a multi-level compaction.
	// Only includes L1 through the last non-snapshot level.
	Levels []*CompactionLevelConfig `yaml:"levels"`

	// Snapshot configuration
	Snapshot SnapshotConfig `yaml:"snapshot"`

	// L0 retention settings
	L0Retention              *time.Duration `yaml:"l0-retention"`
	L0RetentionCheckInterval *time.Duration `yaml:"l0-retention-check-interval"`

	// List of databases to manage.
	DBs []*DBConfig `yaml:"dbs"`

	// Subcommand to execute during replication.
	// Litestream will shutdown when subcommand exits.
	Exec string `yaml:"exec"`

	// Logging
	Logging LoggingConfig `yaml:"logging"`

	// MCP server options
	MCPAddr string `yaml:"mcp-addr"`

	// Path to the config file
	// This is only used internally to pass the config path to the MCP tool
	ConfigPath string `yaml:"-"`
}

// SnapshotConfig configures snapshots.
type SnapshotConfig struct {
	Interval  *time.Duration `yaml:"interval"`
	Retention *time.Duration `yaml:"retention"`
}

// LoggingConfig configures logging.
type LoggingConfig struct {
	Level  string `yaml:"level"`
	Type   string `yaml:"type"`
	Stderr bool   `yaml:"stderr"`
}

// propagateGlobalSettings copies global replica settings to individual replica configs.
func (c *Config) propagateGlobalSettings() {
	for _, dbc := range c.DBs {
		// Handle both old-style 'replicas' and new-style 'replica'
		if dbc.Replica != nil {
			dbc.Replica.SetDefaults(&c.ReplicaSettings)
		}
		for _, rc := range dbc.Replicas {
			rc.SetDefaults(&c.ReplicaSettings)
		}
	}
}

// DefaultConfig returns a new instance of Config with defaults set.
func DefaultConfig() Config {
	defaultSnapshotInterval := 24 * time.Hour
	defaultSnapshotRetention := 24 * time.Hour
	defaultL0Retention := litestream.DefaultL0Retention
	defaultL0RetentionCheckInterval := litestream.DefaultL0RetentionCheckInterval
	return Config{
		Levels: []*CompactionLevelConfig{
			{Interval: 30 * time.Second},
			{Interval: 5 * time.Minute},
			{Interval: 1 * time.Hour},
		},
		Snapshot: SnapshotConfig{
			Interval:  &defaultSnapshotInterval,
			Retention: &defaultSnapshotRetention,
		},
		L0Retention:              &defaultL0Retention,
		L0RetentionCheckInterval: &defaultL0RetentionCheckInterval,
	}
}

// Validate returns an error if config contains invalid settings.
func (c *Config) Validate() error {
	// Validate snapshot intervals
	if c.Snapshot.Interval != nil && *c.Snapshot.Interval <= 0 {
		return &ConfigValidationError{
			Err:   ErrInvalidSnapshotInterval,
			Field: "snapshot.interval",
			Value: *c.Snapshot.Interval,
		}
	}
	if c.Snapshot.Retention != nil && *c.Snapshot.Retention <= 0 {
		return &ConfigValidationError{
			Err:   ErrInvalidSnapshotRetention,
			Field: "snapshot.retention",
			Value: *c.Snapshot.Retention,
		}
	}
	if c.L0Retention != nil && *c.L0Retention <= 0 {
		return &ConfigValidationError{
			Err:   ErrInvalidL0Retention,
			Field: "l0-retention",
			Value: *c.L0Retention,
		}
	}
	if c.L0RetentionCheckInterval != nil && *c.L0RetentionCheckInterval <= 0 {
		return &ConfigValidationError{
			Err:   ErrInvalidL0RetentionCheckInterval,
			Field: "l0-retention-check-interval",
			Value: *c.L0RetentionCheckInterval,
		}
	}

	// Validate compaction level intervals
	for i, level := range c.Levels {
		if level.Interval <= 0 {
			return &ConfigValidationError{
				Err:   ErrInvalidCompactionInterval,
				Field: fmt.Sprintf("levels[%d].interval", i),
				Value: level.Interval,
			}
		}
	}

	// Validate database configs
	for idx, db := range c.DBs {
		// Validate that either path or dir is specified, but not both
		if db.Path != "" && db.Dir != "" {
			return fmt.Errorf("database config #%d: cannot specify both 'path' and 'dir'", idx+1)
		}
		if db.Path == "" && db.Dir == "" {
			return fmt.Errorf("database config #%d: must specify either 'path' or 'dir'", idx+1)
		}

		// When using dir, pattern must be specified
		if db.Dir != "" && db.Pattern == "" {
			return fmt.Errorf("database config #%d: 'pattern' is required when using 'dir'", idx+1)
		}
		if db.Watch && db.Dir == "" {
			return fmt.Errorf("database config #%d: 'watch' can only be enabled with a directory", idx+1)
		}

		// Use path or dir for identifying the config in error messages
		dbIdentifier := db.Path
		if dbIdentifier == "" {
			dbIdentifier = db.Dir
		}

		// Validate sync intervals for replicas
		if db.Replica != nil && db.Replica.SyncInterval != nil && *db.Replica.SyncInterval <= 0 {
			return &ConfigValidationError{
				Err:   ErrInvalidSyncInterval,
				Field: fmt.Sprintf("dbs[%s].replica.sync-interval", dbIdentifier),
				Value: *db.Replica.SyncInterval,
			}
		}
		for i, replica := range db.Replicas {
			if replica.SyncInterval != nil && *replica.SyncInterval <= 0 {
				return &ConfigValidationError{
					Err:   ErrInvalidSyncInterval,
					Field: fmt.Sprintf("dbs[%s].replicas[%d].sync-interval", dbIdentifier, i),
					Value: *replica.SyncInterval,
				}
			}
		}
	}

	return nil
}

// CompactionLevels returns a full list of compaction levels include L0.
func (c *Config) CompactionLevels() litestream.CompactionLevels {
	levels := litestream.CompactionLevels{
		{Level: 0},
	}

	for i, lvl := range c.Levels {
		levels = append(levels, &litestream.CompactionLevel{
			Level:    i + 1,
			Interval: lvl.Interval,
		})
	}

	return levels
}

// DBConfig returns database configuration by path.
func (c *Config) DBConfig(configPath string) *DBConfig {
	for _, dbConfig := range c.DBs {
		if dbConfig.Path == configPath {
			return dbConfig
		}
	}
	return nil
}

// OpenConfigFile opens a configuration file and returns a reader.
// Expands the filename path if needed.
func OpenConfigFile(filename string) (io.ReadCloser, error) {
	// Expand filename, if necessary.
	filename, err := Expand(filename)
	if err != nil {
		return nil, err
	}

	// Open configuration file.
	f, err := os.Open(filename)
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("%w: %s", ErrConfigFileNotFound, filename)
	} else if err != nil {
		return nil, err
	}

	return f, nil
}

// ReadConfigFile unmarshals config from filename. Expands path if needed.
// If expandEnv is true then environment variables are expanded in the config.
func ReadConfigFile(filename string, expandEnv bool) (Config, error) {
	f, err := OpenConfigFile(filename)
	if err != nil {
		return DefaultConfig(), err
	}
	defer f.Close()

	return ParseConfig(f, expandEnv)
}

// ParseConfig unmarshals config from a reader.
// If expandEnv is true then environment variables are expanded in the config.
func ParseConfig(r io.Reader, expandEnv bool) (_ Config, err error) {
	config := DefaultConfig()

	// Read configuration.
	buf, err := io.ReadAll(r)
	if err != nil {
		return config, err
	}

	// Expand environment variables, if enabled.
	if expandEnv {
		buf = []byte(os.ExpandEnv(string(buf)))
	}

	// Save defaults before unmarshaling
	defaultSnapshotInterval := config.Snapshot.Interval
	defaultSnapshotRetention := config.Snapshot.Retention
	defaultL0Retention := config.L0Retention
	defaultL0RetentionCheckInterval := config.L0RetentionCheckInterval

	if err := yaml.Unmarshal(buf, &config); err != nil {
		return config, err
	}

	// Restore defaults if they were overwritten with nil by empty YAML sections
	if config.Snapshot.Interval == nil {
		config.Snapshot.Interval = defaultSnapshotInterval
	}
	if config.Snapshot.Retention == nil {
		config.Snapshot.Retention = defaultSnapshotRetention
	}
	if config.L0Retention == nil {
		config.L0Retention = defaultL0Retention
	}
	if config.L0RetentionCheckInterval == nil {
		config.L0RetentionCheckInterval = defaultL0RetentionCheckInterval
	}

	// Normalize paths.
	for _, dbConfig := range config.DBs {
		if dbConfig.Path == "" {
			continue
		}
		if dbConfig.Path, err = Expand(dbConfig.Path); err != nil {
			return config, err
		}
	}

	// Propage settings from global config to replica configs.
	config.propagateGlobalSettings()

	// Validate configuration
	if err := config.Validate(); err != nil {
		return config, err
	}

	// logging.
	if v := os.Getenv("LOG_LEVEL"); v != "" {
		config.Logging.Level = v
	}

	return config, nil
}

// CompactionLevelConfig the configuration for a single level of compaction.
type CompactionLevelConfig struct {
	Interval time.Duration `yaml:"interval"`
}

// DBConfig represents the configuration for a single database or directory of databases.
type DBConfig struct {
	Path               string         `yaml:"path"`
	Dir                string         `yaml:"dir"`       // Directory to scan for databases
	Pattern            string         `yaml:"pattern"`   // File pattern to match (e.g., "*.db", "*.sqlite")
	Recursive          bool           `yaml:"recursive"` // Scan subdirectories recursively
	Watch              bool           `yaml:"watch"`     // Enable directory monitoring for changes
	MetaPath           *string        `yaml:"meta-path"`
	MonitorInterval    *time.Duration `yaml:"monitor-interval"`
	CheckpointInterval *time.Duration `yaml:"checkpoint-interval"`
	BusyTimeout        *time.Duration `yaml:"busy-timeout"`
	MinCheckpointPageN *int           `yaml:"min-checkpoint-page-count"`
	TruncatePageN      *int           `yaml:"truncate-page-n"`

	Replica  *ReplicaConfig   `yaml:"replica"`
	Replicas []*ReplicaConfig `yaml:"replicas"` // Deprecated
}

// ByteSize is a custom type for parsing byte sizes from YAML.
// It supports both SI units (KB, MB, GB using base 1000) and IEC units
// (KiB, MiB, GiB using base 1024) as well as short forms (K, M, G).
type ByteSize int64

// UnmarshalYAML implements yaml.Unmarshaler for ByteSize.
func (b *ByteSize) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}

	size, err := ParseByteSize(s)
	if err != nil {
		return err
	}
	*b = ByteSize(size)
	return nil
}

// ParseByteSize parses a byte size string using github.com/dustin/go-humanize.
// Supports both SI units (KB=1000, MB=1000², etc.) and IEC units (KiB=1024, MiB=1024², etc.).
// Examples: "1MB", "5MiB", "1.5GB", "100B", "1024KB"
func ParseByteSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty size string")
	}

	// Use go-humanize to parse the byte size string
	bytes, err := humanize.ParseBytes(s)
	if err != nil {
		return 0, fmt.Errorf("invalid size format: %w", err)
	}

	// Check that the value fits in int64
	if bytes > math.MaxInt64 {
		return 0, fmt.Errorf("size %d exceeds maximum allowed value (%d)", bytes, int64(math.MaxInt64))
	}

	return int64(bytes), nil
}

// ReplicaSettings contains settings shared across replica configurations.
// These can be set globally in Config or per-replica in ReplicaConfig.
type ReplicaSettings struct {
	SyncInterval       *time.Duration `yaml:"sync-interval"`
	ValidationInterval *time.Duration `yaml:"validation-interval"`

	// S3 settings
	AccessKeyID       string    `yaml:"access-key-id"`
	SecretAccessKey   string    `yaml:"secret-access-key"`
	Region            string    `yaml:"region"`
	Bucket            string    `yaml:"bucket"`
	Endpoint          string    `yaml:"endpoint"`
	ForcePathStyle    *bool     `yaml:"force-path-style"`
	SignPayload       *bool     `yaml:"sign-payload"`
	RequireContentMD5 *bool     `yaml:"require-content-md5"`
	SkipVerify        bool      `yaml:"skip-verify"`
	PartSize          *ByteSize `yaml:"part-size"`
	Concurrency       *int      `yaml:"concurrency"`

	// ABS settings
	AccountName string `yaml:"account-name"`
	AccountKey  string `yaml:"account-key"`

	// SFTP settings
	Host             string `yaml:"host"`
	User             string `yaml:"user"`
	Password         string `yaml:"password"`
	KeyPath          string `yaml:"key-path"`
	ConcurrentWrites *bool  `yaml:"concurrent-writes"`
	HostKey          string `yaml:"host-key"`

	// WebDAV settings
	WebDAVURL      string `yaml:"webdav-url"`
	WebDAVUsername string `yaml:"webdav-username"`
	WebDAVPassword string `yaml:"webdav-password"`

	// NATS settings
	JWT           string         `yaml:"jwt"`
	Seed          string         `yaml:"seed"`
	Creds         string         `yaml:"creds"`
	NKey          string         `yaml:"nkey"`
	Username      string         `yaml:"username"`
	Token         string         `yaml:"token"`
	TLS           bool           `yaml:"tls"`
	RootCAs       []string       `yaml:"root-cas"`
	ClientCert    string         `yaml:"client-cert"`
	ClientKey     string         `yaml:"client-key"`
	MaxReconnects *int           `yaml:"max-reconnects"`
	ReconnectWait *time.Duration `yaml:"reconnect-wait"`
	Timeout       *time.Duration `yaml:"timeout"`

	// Encryption identities and recipients
	Age struct {
		Identities []string `yaml:"identities"`
		Recipients []string `yaml:"recipients"`
	} `yaml:"age"`
}

// SetDefaults merges default settings from src into the current ReplicaSettings.
// Individual settings override defaults when already set.
func (rs *ReplicaSettings) SetDefaults(src *ReplicaSettings) {
	if src == nil {
		return
	}

	// Timing settings
	if rs.SyncInterval == nil && src.SyncInterval != nil {
		rs.SyncInterval = src.SyncInterval
	}
	if rs.ValidationInterval == nil && src.ValidationInterval != nil {
		rs.ValidationInterval = src.ValidationInterval
	}

	// S3 settings
	if rs.AccessKeyID == "" {
		rs.AccessKeyID = src.AccessKeyID
	}
	if rs.SecretAccessKey == "" {
		rs.SecretAccessKey = src.SecretAccessKey
	}
	if rs.Region == "" {
		rs.Region = src.Region
	}
	if rs.Bucket == "" {
		rs.Bucket = src.Bucket
	}
	if rs.Endpoint == "" {
		rs.Endpoint = src.Endpoint
	}
	if rs.ForcePathStyle == nil {
		rs.ForcePathStyle = src.ForcePathStyle
	}
	if rs.SignPayload == nil {
		rs.SignPayload = src.SignPayload
	}
	if rs.RequireContentMD5 == nil {
		rs.RequireContentMD5 = src.RequireContentMD5
	}
	if src.SkipVerify {
		rs.SkipVerify = true
	}

	// ABS settings
	if rs.AccountName == "" {
		rs.AccountName = src.AccountName
	}
	if rs.AccountKey == "" {
		rs.AccountKey = src.AccountKey
	}

	// SFTP settings
	if rs.Host == "" {
		rs.Host = src.Host
	}
	if rs.User == "" {
		rs.User = src.User
	}
	if rs.Password == "" {
		rs.Password = src.Password
	}
	if rs.KeyPath == "" {
		rs.KeyPath = src.KeyPath
	}
	if rs.ConcurrentWrites == nil {
		rs.ConcurrentWrites = src.ConcurrentWrites
	}

	// NATS settings
	if rs.JWT == "" {
		rs.JWT = src.JWT
	}
	if rs.Seed == "" {
		rs.Seed = src.Seed
	}
	if rs.Creds == "" {
		rs.Creds = src.Creds
	}
	if rs.NKey == "" {
		rs.NKey = src.NKey
	}
	if rs.Username == "" {
		rs.Username = src.Username
	}
	if rs.Token == "" {
		rs.Token = src.Token
	}
	if !rs.TLS {
		rs.TLS = src.TLS
	}
	if len(rs.RootCAs) == 0 {
		rs.RootCAs = src.RootCAs
	}
	if rs.ClientCert == "" {
		rs.ClientCert = src.ClientCert
	}
	if rs.ClientKey == "" {
		rs.ClientKey = src.ClientKey
	}
	if rs.MaxReconnects == nil {
		rs.MaxReconnects = src.MaxReconnects
	}
	if rs.ReconnectWait == nil {
		rs.ReconnectWait = src.ReconnectWait
	}
	if rs.Timeout == nil {
		rs.Timeout = src.Timeout
	}

	// Age encryption settings
	if len(rs.Age.Identities) == 0 {
		rs.Age.Identities = src.Age.Identities
	}
	if len(rs.Age.Recipients) == 0 {
		rs.Age.Recipients = src.Age.Recipients
	}
}

// ReplicaConfig represents the configuration for a single replica in a database.
type ReplicaConfig struct {
	ReplicaSettings `yaml:",inline"`

	Type string `yaml:"type"` // "file", "s3"
	Name string `yaml:"name"` // Deprecated
	Path string `yaml:"path"`
	URL  string `yaml:"url"`
}

// ReplicaType returns the type based on the type field or extracted from the URL.
func (c *ReplicaConfig) ReplicaType() string {
	if replicaType := litestream.ReplicaTypeFromURL(c.URL); replicaType != "" {
		return replicaType
	} else if c.Type != "" {
		return c.Type
	}
	return "file"
}


// expand returns an absolute path for s.
// It also strips SQLite connection string prefixes (sqlite://, sqlite3://).
func Expand(s string) (string, error) {
	// Strip SQLite connection string prefixes if present.
	s = StripSQLitePrefix(s)

	// Just expand to absolute path if there is no home directory prefix.
	prefix := "~" + string(os.PathSeparator)
	if s != "~" && !strings.HasPrefix(s, prefix) {
		return filepath.Abs(s)
	}

	// Look up home directory.
	u, err := user.Current()
	if err != nil {
		return "", err
	} else if u.HomeDir == "" {
		return "", fmt.Errorf("cannot expand path %s, no home directory available", s)
	}

	// Return path with tilde replaced by the home directory.
	if s == "~" {
		return u.HomeDir, nil
	}
	return filepath.Join(u.HomeDir, strings.TrimPrefix(s, prefix)), nil
}

// StripSQLitePrefix removes SQLite connection string prefixes (sqlite://, sqlite3://)
// from the given path. This allows users to use standard connection string formats
// across their tooling while Litestream extracts just the file path.
func StripSQLitePrefix(s string) string {
	if len(s) < 9 || s[0] != 's' {
		return s
	}
	for _, prefix := range []string{"sqlite3://", "sqlite://"} {
		if strings.HasPrefix(s, prefix) {
			return strings.TrimPrefix(s, prefix)
		}
	}
	return s
}

