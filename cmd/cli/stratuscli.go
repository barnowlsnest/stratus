package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/barnowlsnest/stratus/pkg/stratusv1"
)

func main() {
	client, err := stratusv1.Dial("", stratusv1.WithInsecure())
	if err != nil {
		log.Fatal("failed to init stratus client: ", err)
	}

	defer func() { _ = client.Close() }()

	mainCtx := context.Background()
	_, cancel := signal.NotifyContext(mainCtx, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer cancel()

}
