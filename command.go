package consistenthashing

import (
	"context"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func RunCommand(cmd func(cmd *cobra.Command, args []string) error) {
	signals := make(chan os.Signal, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		select {
		case sig := <-signals:
			log.WithField("signal", sig).Info("got signal, clean up and exit")
			cancel()
		case <-ctx.Done():
		}
	}()

	rootCmd := &cobra.Command{
		PreRun: func(cmd *cobra.Command, args []string) {
			rand.Seed(time.Now().UTC().UnixNano())

			viper.AutomaticEnv()
			viper.SetDefault("rabbitmq_url", "amqp://test:test123@localhost:5672/")

			signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
		},
		RunE: cmd,
		PostRun: func(cmd *cobra.Command, args []string) {
			signal.Stop(signals)
		},
	}

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		log.WithError(err).Fatal("failed to execute command")
	}
}
