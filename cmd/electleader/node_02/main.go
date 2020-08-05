package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/amazingchow/snippets-for-gopher/electleader"
)

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})

	cfg := &electleader.ElectorCfg{
		ZKEndpoints:       []string{"127.0.0.1:2181", "127.0.0.1:2182", "127.0.0.1:2183"},
		ElectionHeartbeat: 2,
	}
	e := electleader.NewElector(cfg)
	go e.ElectLeader("node_02")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	for range sigCh {
		e.Close()
		break
	}
}
