package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/barnowlsnest/stratus/cmd/cli/commands"
	"github.com/barnowlsnest/stratus/cmd/cli/options"
	"github.com/barnowlsnest/stratus/pkg/stratusv1"
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

	client, err := dial(opts)
	if err != nil {
		return fmt.Errorf("failed to dial stratus: %w", err)
	}

	defer func() { _ = client.Close() }()

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

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		return fmt.Errorf("failed to execute command: %w", err)
	}

	return nil
}

func dial(opts *options.Options) (*stratusv1.Client, error) {
	addr := net.JoinHostPort(opts.Host, strconv.Itoa(opts.Port))
	return stratusv1.Dial(addr, stratusv1.WithInsecure())
}
