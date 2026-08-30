package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/charmbracelet/colorprofile"
	"github.com/spf13/pflag"

	"github.com/barnowlsnest/stratus/cmd/cli/commands"
	"github.com/barnowlsnest/stratus/cmd/cli/options"
	"github.com/barnowlsnest/stratus/cmd/cli/tui"
	"github.com/barnowlsnest/stratus/pkg/stratusv1"

	tea "charm.land/bubbletea/v2"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	opts, globalFlags, err := options.Load()
	if err != nil {
		return fmt.Errorf("failed to load options: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	addr := net.JoinHostPort(opts.Host, strconv.Itoa(opts.Port))
	client, err := stratusv1.Dial(addr, stratusv1.WithInsecure())
	if err != nil {
		return fmt.Errorf("failed to dial stratus: %w", err)
	}

	defer func() { _ = client.Close() }()

	if opts.IsTUI {
		return runTUI(ctx, client, opts)
	}

	return runOnlyCommands(ctx, globalFlags, client)
}

func runTUI(ctx context.Context, client *stratusv1.Client, opts *options.Options) error {
	p := tea.NewProgram(
		tui.NewModel(ctx, client, opts),
		tea.WithContext(ctx),
		tea.WithColorProfile(colorprofile.ANSI256),
	)

	_, err := p.Run()
	if err != nil {
		return fmt.Errorf("failed to run TUI mode: %w", err)
	}

	return nil
}

func runOnlyCommands(ctx context.Context, globalFlags *pflag.FlagSet, client *stratusv1.Client) error {
	rootCmd := commands.NewRoot(globalFlags)
	rootCmd.AddCommand(commands.NewInfo(client))
	rootCmd.AddCommand(commands.NewReconcileCache(client))

	addCmd, err := commands.NewAdd(client)
	if err != nil {
		return fmt.Errorf("failed to create add command: %w", err)
	}

	rootCmd.AddCommand(addCmd)

	deleteCmd, err := commands.NewDelete(client)
	if err != nil {
		return fmt.Errorf("failed to create delete command: %w", err)
	}

	rootCmd.AddCommand(deleteCmd)

	getCmd, err := commands.NewGet(client)
	if err != nil {
		return fmt.Errorf("failed to create get command: %w", err)
	}

	rootCmd.AddCommand(getCmd)

	offsetCmd, err := commands.NewOffset(client)
	if err != nil {
		return fmt.Errorf("failed to create offset command: %w", err)
	}

	rootCmd.AddCommand(offsetCmd)

	addFile, err := commands.NewAddFile(client)
	if err != nil {
		return fmt.Errorf("failed to create addfile command: %w", err)
	}

	rootCmd.AddCommand(addFile)

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		return fmt.Errorf("failed to execute command: %w", err)
	}

	return nil
}
