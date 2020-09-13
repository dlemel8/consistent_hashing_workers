package consistenthashing

import "github.com/spf13/viper"

func SetupConfig() {
	viper.AutomaticEnv()
	viper.SetDefault("rabbitmq_url", "amqp://test:test123@localhost:5672/")
}
