package setup

import (
	"fmt"
	"strings"

	"path"
	"path/filepath"
	"os"

	"net/url"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/config"
)

// NewDBFromConfig instantiates a DB based on a configuration.
func NewDBFromConfig(dbc *config.DBConfig) (*litestream.DB, error) {
	configPath, err := config.Expand(dbc.Path)
	if err != nil {
		return nil, err
	}

	// Initialize database with given path.
	db := litestream.NewDB(configPath)

	// Override default database settings if specified in configuration.
	if dbc.MetaPath != nil {
		expandedMetaPath, err := config.Expand(*dbc.MetaPath)
		if err != nil {
			return nil, fmt.Errorf("failed to expand meta path: %w", err)
		}
		dbc.MetaPath = &expandedMetaPath
		db.SetMetaPath(expandedMetaPath)
	}
	if dbc.MonitorInterval != nil {
		db.MonitorInterval = *dbc.MonitorInterval
	}
	if dbc.CheckpointInterval != nil {
		db.CheckpointInterval = *dbc.CheckpointInterval
	}
	if dbc.BusyTimeout != nil {
		db.BusyTimeout = *dbc.BusyTimeout
	}
	if dbc.MinCheckpointPageN != nil {
		db.MinCheckpointPageN = *dbc.MinCheckpointPageN
	}
	if dbc.TruncatePageN != nil {
		db.TruncatePageN = *dbc.TruncatePageN
	}

	// Instantiate and attach replica.
	// v0.3.x and before supported multiple replicas but that was dropped to
	// ensure there's a single remote data authority.
	switch {
	case dbc.Replica == nil && len(dbc.Replicas) == 0:
		return nil, fmt.Errorf("must specify replica for database")
	case dbc.Replica != nil && len(dbc.Replicas) > 0:
		return nil, fmt.Errorf("cannot specify 'replica' and 'replicas' on a database")
	case len(dbc.Replicas) > 1:
		return nil, fmt.Errorf("multiple replicas on a single database are no longer supported")
	}

	var rc *config.ReplicaConfig
	if dbc.Replica != nil {
		rc = dbc.Replica
	} else {
		rc = dbc.Replicas[0]
	}

	r, err := NewReplicaFromConfig(rc, db)
	if err != nil {
		return nil, err
	}
	db.Replica = r

	return db, nil
}

// NewDBsFromDirectoryConfig scans a directory and creates DB instances for all SQLite databases found.
func NewDBsFromDirectoryConfig(dbc *config.DBConfig) ([]*litestream.DB, error) {
	if dbc.Dir == "" {
		return nil, fmt.Errorf("directory path is required for directory replication")
	}

	if dbc.Pattern == "" {
		return nil, fmt.Errorf("pattern is required for directory replication")
	}

	dirPath, err := config.Expand(dbc.Dir)
	if err != nil {
		return nil, err
	}

	// Find all SQLite databases in the directory
	dbPaths, err := FindSQLiteDatabases(dirPath, dbc.Pattern, dbc.Recursive)
	if err != nil {
		return nil, fmt.Errorf("failed to scan directory %s: %w", dirPath, err)
	}

	if len(dbPaths) == 0 && !dbc.Watch {
		return nil, fmt.Errorf("no SQLite databases found in directory %s with pattern %s", dirPath, dbc.Pattern)
	}

	// Create DB instances for each found database
	var dbs []*litestream.DB
	metaPaths := make(map[string]string)

	for _, dbPath := range dbPaths {
		db, err := newDBFromDirectoryEntry(dbc, dirPath, dbPath)
		if err != nil {
			return nil, fmt.Errorf("failed to create DB for %s: %w", dbPath, err)
		}

		// Validate unique meta-path to prevent replication state corruption
		if mp := db.MetaPath(); mp != "" {
			if existingDB, exists := metaPaths[mp]; exists {
				return nil, fmt.Errorf("meta-path collision: databases %s and %s would share meta-path %s, causing replication state corruption", existingDB, dbPath, mp)
			}
			metaPaths[mp] = dbPath
		}

		dbs = append(dbs, db)
	}

	return dbs, nil
}

// newDBFromDirectoryEntry creates a DB instance for a database discovered via directory replication.
func newDBFromDirectoryEntry(dbc *config.DBConfig, dirPath, dbPath string) (*litestream.DB, error) {
	// Calculate relative path from directory root
	relPath, err := filepath.Rel(dirPath, dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate relative path for %s: %w", dbPath, err)
	}

	// Create a copy of the config for the discovered database
	dbConfigCopy := *dbc
	dbConfigCopy.Path = dbPath
	dbConfigCopy.Dir = ""          // Clear dir field for individual DB
	dbConfigCopy.Pattern = ""      // Clear pattern field
	dbConfigCopy.Recursive = false // Clear recursive flag
	dbConfigCopy.Watch = false     // Individual DBs do not watch directories

	// Ensure every database discovered beneath a directory receives a unique
	// metadata path. Without this, all databases share the same meta-path and
	// clobber each other's replication state.
	if dbc.MetaPath != nil {
		baseMetaPath, err := config.Expand(*dbc.MetaPath)
		if err != nil {
			return nil, fmt.Errorf("failed to expand meta path for %s: %w", dbPath, err)
		}
		metaPathCopy := deriveMetaPathForDirectoryEntry(baseMetaPath, relPath)
		dbConfigCopy.MetaPath = &metaPathCopy
	}

	// Deep copy replica config and make path unique per database.
	// This prevents all databases from writing to the same replica path.
	if dbc.Replica != nil {
		replicaCopy, err := cloneReplicaConfigWithRelativePath(dbc.Replica, relPath)
		if err != nil {
			return nil, fmt.Errorf("failed to configure replica for %s: %w", dbPath, err)
		}
		dbConfigCopy.Replica = replicaCopy
	}

	// Also handle deprecated 'replicas' array field.
	if len(dbc.Replicas) > 0 {
		dbConfigCopy.Replicas = make([]*config.ReplicaConfig, len(dbc.Replicas))
		for i, replica := range dbc.Replicas {
			replicaCopy, err := cloneReplicaConfigWithRelativePath(replica, relPath)
			if err != nil {
				return nil, fmt.Errorf("failed to configure replica %d for %s: %w", i, dbPath, err)
			}
			dbConfigCopy.Replicas[i] = replicaCopy
		}
	}

	return NewDBFromConfig(&dbConfigCopy)
}

// cloneReplicaConfigWithRelativePath returns a copy of the replica configuration with the
// database-relative path appended to either the replica path or URL, depending on how the
// replica was configured.
func cloneReplicaConfigWithRelativePath(base *config.ReplicaConfig, relPath string) (*config.ReplicaConfig, error) {
	if base == nil {
		return nil, nil
	}

	replicaCopy := *base
	relPath = filepath.ToSlash(relPath)
	if relPath == "" || relPath == "." {
		return &replicaCopy, nil
	}

	if replicaCopy.URL != "" {
		u, err := url.Parse(replicaCopy.URL)
		if err != nil {
			return nil, fmt.Errorf("parse replica url: %w", err)
		}
		appendRelativePathToURL(u, relPath)
		replicaCopy.URL = u.String()
		return &replicaCopy, nil
	}

	switch base.ReplicaType() {
	case "file":
		relOSPath := filepath.FromSlash(relPath)
		if replicaCopy.Path != "" {
			replicaCopy.Path = filepath.Join(replicaCopy.Path, relOSPath)
		} else {
			replicaCopy.Path = relOSPath
		}
	default:
		// Normalize to forward slashes for cloud/object storage backends.
		basePath := filepath.ToSlash(replicaCopy.Path)
		if basePath != "" {
			replicaCopy.Path = path.Join(basePath, relPath)
		} else {
			replicaCopy.Path = relPath
		}
	}

	return &replicaCopy, nil
}

// FindSQLiteDatabases recursively finds all SQLite database files in a directory.
// Exported for testing.
func FindSQLiteDatabases(dir string, pattern string, recursive bool) ([]string, error) {
	var dbPaths []string

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories unless recursive
		if info.IsDir() {
			if !recursive && path != dir {
				return filepath.SkipDir
			}
			return nil
		}

		// Check if file matches pattern
		matched, err := filepath.Match(pattern, filepath.Base(path))
		if err != nil {
			return err
		}
		if !matched {
			return nil
		}

		// Check if it's a SQLite database
		if IsSQLiteDatabase(path) {
			dbPaths = append(dbPaths, path)
		}

		return nil
	})

	return dbPaths, err
}

// IsSQLiteDatabase checks if a file is a SQLite database by reading its header.
// Exported for testing.
func IsSQLiteDatabase(path string) bool {
	file, err := os.Open(path)
	if err != nil {
		return false
	}
	defer file.Close()

	// SQLite files start with "SQLite format 3\x00"
	header := make([]byte, 16)
	if _, err := file.Read(header); err != nil {
		return false
	}

	return string(header) == "SQLite format 3\x00"
}

// deriveMetaPathForDirectoryEntry returns a unique metadata directory for a
// database discovered through directory replication by appending the database's
// relative path and the standard Litestream suffix to the configured base path.
func deriveMetaPathForDirectoryEntry(basePath, relPath string) string {
	relPath = filepath.Clean(relPath)
	if relPath == "." || relPath == "" {
		return basePath
	}

	relDir, relFile := filepath.Split(relPath)
	if relFile == "" || relFile == "." {
		return filepath.Join(basePath, relPath)
	}

	metaDirName := "." + relFile + litestream.MetaDirSuffix
	return filepath.Join(basePath, relDir, metaDirName)
}

// appendRelativePathToURL appends relPath to the URL's path component, ensuring
// the result remains rooted and uses forward slashes.
func appendRelativePathToURL(u *url.URL, relPath string) {
	cleanRel := strings.TrimPrefix(relPath, "/")
	if cleanRel == "" || cleanRel == "." {
		return
	}

	basePath := u.Path
	var joined string
	if basePath == "" {
		joined = cleanRel
	} else {
		joined = path.Join(basePath, cleanRel)
	}

	joined = "/" + strings.TrimPrefix(joined, "/")
	u.Path = joined
}
