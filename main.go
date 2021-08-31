package main

import (
	"os"
	"os/signal"
	"syscall"

	"sync"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/urfave/cli"
)

func appCLISetup() *cli.App {
	app := cli.NewApp()

	app.Name = "Kafka ES SFlow consumer"
	app.Usage = "Just run it"
	app.Email = "egor.krv@gmail.com"
	app.Version = "0.1"

	return app
}

type Cfg struct {
	KafkaCfg KafkaConfig
	ESCfg    ESCfg
}

func readCfg() (*Cfg, error) {
	var cfg Cfg

	viper.SetConfigName("ksflowes")
	viper.AddConfigPath(".")
	viper.SetConfigType("toml")

	err := viper.ReadInConfig()
	if err != nil {
		return nil, err
	}

	cfg.KafkaCfg.User = viper.GetString("kafka.user")
	cfg.KafkaCfg.Password = viper.GetString("kafka.password")
	cfg.KafkaCfg.BrokerList = viper.GetStringSlice("kafka.brokers")
	cfg.KafkaCfg.GroupName = viper.GetString("kafka.group")
	cfg.KafkaCfg.Topic = viper.GetString("kafka.topic")

	cfg.ESCfg.Address = viper.GetString("elastic.address")
	cfg.ESCfg.Port = viper.GetString("elastic.port")
	cfg.ESCfg.IPDBPath = viper.GetString("elastic.ipdb")
	cfg.ESCfg.ASNDBPath = viper.GetString("elastic.asndb")

	cfg.ESCfg.MacTable = viper.GetStringMapString("mac")
	cfg.ESCfg.GeoIPEnabled = viper.GetBool("elastic.geoip")

	return &cfg, nil
}

func watchSigTerm(c *chan struct{}) {

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, os.Interrupt)

	<-signals

	logrus.WithFields(logrus.Fields{
		"topic": "shutdown",
		"event": "closing",
	}).Info("initiating shutdown of consumer...")

	close(*c)
}

func main() {

	logrus.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})

	cfg, err := readCfg()
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"topic": "config",
			"event": "readCfg",
		}).Fatal(err)
	}

	app := appCLISetup()
	app.Action = func(c *cli.Context) {
		logrus.WithFields(logrus.Fields{
			"topic": "start",
			"event": "starting",
		}).Info("initiating consumer...")

		var wg sync.WaitGroup

		k, err := InitKafka(cfg.KafkaCfg)
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"topic": "main",
				"event": "InitKafka",
			}).Fatal(err)
		}

		go watchSigTerm(k.Closing)

		err = k.ConsumeAndIndex(&wg)
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"topic": "main",
				"event": "ConsumeAndIndex",
			}).Fatal(err)
		}

		es, err := NewIndexer(cfg.ESCfg, k.Messages)
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"topic": "main",
				"event": "NewIndexer",
			}).Fatal(err)
		}

		go es.Index()

		wg.Wait()

		close(k.Messages)

		err = k.Consumer.Close()
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"topic": "main",
				"event": "Consumer.Close",
			}).Fatal(err)
		}

	}
	err = app.Run(os.Args)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"topic": "app",
			"event": "Run",
		}).Fatal(err)
	}
}
