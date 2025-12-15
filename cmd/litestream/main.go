package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/superfly/ltx"
	_ "modernc.org/sqlite"
)

// Build information.
var (
	Version = "(development build)"
)

// errStop is a terminal error for indicating program should quit.
var errStop = errors.New("stop")


func main() {
	m := NewMain()
	if err := m.Run(context.Background(), os.Args[1:]); errors.Is(err, flag.ErrHelp) || errors.Is(err, errStop) {
		os.Exit(1)
	} else if err != nil {
		slog.Error("failed to run", "error", err)
		os.Exit(1)
	}
}

// Main represents the main program execution.
type Main struct{}

// NewMain returns a new instance of Main.
func NewMain() *Main {
	return &Main{}
}

// Run executes the program.
func (m *Main) Run(ctx context.Context, args []string) (err error) {
	// Execute replication command if running as a Windows service.
	if isService, err := isWindowsService(); err != nil {
		return err
	} else if isService {
		return runWindowsService(ctx)
	}

	// Copy "LITESTEAM" environment credentials.
	applyLitestreamEnv()

	// Extract command name.
	var cmd string
	if len(args) > 0 {
		cmd, args = args[0], args[1:]
	}

	switch cmd {
	case "databases":
		return (&DatabasesCommand{}).Run(ctx, args)
	case "replicate":
		c := NewReplicateCommand()
		if err := c.ParseFlags(ctx, args); err != nil {
			return err
		}

		// Setup signal handler.
		signalCh := signalChan()

		if err := c.Run(ctx); err != nil {
			return err
		}

		// Wait for signal to stop program.
		select {
		case err = <-c.execCh:
			slog.Info("subprocess exited, litestream shutting down")
		case sig := <-signalCh:
			slog.Info("signal received, litestream shutting down")

			if c.cmd != nil {
				slog.Info("sending signal to exec process")
				if err := c.cmd.Process.Signal(sig); err != nil {
					return fmt.Errorf("cannot signal exec process: %w", err)
				}

				slog.Info("waiting for exec process to close")
				if err := <-c.execCh; err != nil && !strings.HasPrefix(err.Error(), "signal:") {
					return fmt.Errorf("cannot wait for exec process: %w", err)
				}
			}
		}

		// Gracefully close.
		if e := c.Close(ctx); e != nil && err == nil {
			err = e
		}
		slog.Info("litestream shut down")
		return err

	case "restore":
		return (&RestoreCommand{}).Run(ctx, args)
	case "version":
		return (&VersionCommand{}).Run(ctx, args)
	case "ltx":
		return (&LTXCommand{}).Run(ctx, args)
	case "wal":
		// Deprecated: Keep for backward compatibility
		fmt.Fprintln(os.Stderr, "Warning: 'wal' command is deprecated, please use 'ltx' instead")
		return (&LTXCommand{}).Run(ctx, args)
	default:
		if cmd == "" || cmd == "help" || strings.HasPrefix(cmd, "-") {
			m.Usage()
			return flag.ErrHelp
		}
		return fmt.Errorf("litestream %s: unknown command", cmd)
	}
}

// Usage prints the help screen to STDOUT.
func (m *Main) Usage() {
	fmt.Println(`
litestream is a tool for replicating SQLite databases.

Usage:

	litestream <command> [arguments]

The commands are:

	databases    list databases specified in config file
	ltx          list available LTX files for a database
	replicate    runs a server to replicate databases
	restore      recovers database backup from a replica
	version      prints the binary version
`[1:])
}

// applyLitestreamEnv copies "LITESTREAM" prefixed environment variables to
// their AWS counterparts as the "AWS" prefix can be confusing when using a
// non-AWS S3-compatible service.
func applyLitestreamEnv() {
	if v, ok := os.LookupEnv("LITESTREAM_ACCESS_KEY_ID"); ok {
		if _, ok := os.LookupEnv("AWS_ACCESS_KEY_ID"); !ok {
			os.Setenv("AWS_ACCESS_KEY_ID", v)
		}
	}
	if v, ok := os.LookupEnv("LITESTREAM_SECRET_ACCESS_KEY"); ok {
		if _, ok := os.LookupEnv("AWS_SECRET_ACCESS_KEY"); !ok {
			os.Setenv("AWS_SECRET_ACCESS_KEY", v)
		}
	}
}


// DefaultConfigPath returns the default config path.
func DefaultConfigPath() string {
	if v := os.Getenv("LITESTREAM_CONFIG"); v != "" {
		return v
	}
	return defaultConfigPath
}

func registerConfigFlag(fs *flag.FlagSet) (configPath *string, noExpandEnv *bool) {
	return fs.String("config", "", "config path"),
		fs.Bool("no-expand-env", false, "do not expand env vars in config")
}


// txidVar allows the flag package to parse index flags as hex-formatted TXIDs
type txidVar ltx.TXID

// Ensure type implements interface.
var _ flag.Value = (*txidVar)(nil)

// String returns an 8-character hexadecimal value.
func (v *txidVar) String() string {
	return ltx.TXID(*v).String()
}

// Set parses s into an integer from a hexadecimal value.
func (v *txidVar) Set(s string) error {
	txID, err := ltx.ParseTXID(s)
	if err != nil {
		return fmt.Errorf("invalid txid format")
	}
	*v = txidVar(txID)
	return nil
}

