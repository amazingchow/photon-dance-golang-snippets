package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	leaderelect "github.com/amazingchow/photon-dance-golang-snippets/gocql-leaderelect"
)

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})

	cfg := leaderelect.NewConfig("c_node_03", "leader_elect_resource")
	cfg.Hosts = []string{"127.0.0.1:19041", "127.0.0.1:19042", "127.0.0.1:19043"}
	cfg.AdvertiseAddress = "127.0.0.1:18083"

	le, err := leaderelect.NewElector(cfg)
	if err != nil {
		panic(err)
	}

	le.Start()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

MAIN_LOOP:
	for {
		select {
		case <-sigCh:
			{
				break MAIN_LOOP
			}
		case s, ok := <-le.Status():
			if !ok {
				break MAIN_LOOP
			}
			log.Info().Msgf("pid: %d, status: %+v\n", os.Getpid(), s)
		}
	}

	le.Resign()
}
