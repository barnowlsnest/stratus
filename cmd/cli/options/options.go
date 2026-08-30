package options

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/barnowlsnest/go-configlib/v2/pkg/configs"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type Options struct {
	LogLevel string `name:"loglevel" default:"info" usage:"log level for the application"`
	Host     string `name:"host" default:"127.0.0.1" usage:"stratus hostname"`
	Port     int    `name:"port" default:"8000" usage:"stratus port"`
	TUI      bool   `name:"tui" default:"false" usage:"enable TUI"`

	config *configs.Config
}

// Load resolves the global options from CLI flags, env vars and defaults, so
// they are usable before the command tree runs. It parses os.Args itself,
// ignoring the flags it does not own since those belong to the subcommands.
// The returned FlagSet is meant to be attached to the root command, which
// reparses and validates the very same flags.
func Load() (opts *Options, globalFlags *pflag.FlagSet, err error) {
	var cfg Options

	globalFlags = pflag.NewFlagSet("global", pflag.ContinueOnError)
	globalFlags.ParseErrorsAllowlist.UnknownFlags = true
	globalFlags.SetOutput(io.Discard)
	globalFlags.Usage = func() {}

	cfg.config = viper.New()
	if err = configs.Register(cfg.config, globalFlags, &cfg); err != nil {
		return nil, nil, fmt.Errorf("register global flags: %w", err)
	}

	// ErrHelp is not a failure: -h/--help is handled by cobra, which owns this
	// flag set next and prints the usage.
	if err = globalFlags.Parse(os.Args[1:]); err != nil && !errors.Is(err, pflag.ErrHelp) {
		return nil, nil, fmt.Errorf("parse global flags: %w", err)
	}

	if err = cfg.config.BindPFlags(globalFlags); err != nil {
		return nil, nil, fmt.Errorf("bind global flags: %w", err)
	}

	if err = configs.Load(cfg.config, &cfg); err != nil {
		return nil, nil, fmt.Errorf("load options: %w", err)
	}

	return &cfg, globalFlags, nil
}

func (o *Options) Config() *configs.Config {
	return o.config
}
