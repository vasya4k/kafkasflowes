package main

import (
	"crypto/tls"
	"encoding/json"
	fmt "fmt"
	"log"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
)

type KafkaConfig struct {
	Topic      string
	User       string
	Password   string
	BrokerList []string
	GroupName  string
	SASL       bool
}

type ConsumeManager struct {
	pc sarama.PartitionConsumer
	pm sarama.PartitionOffsetManager
}

type Kafka struct {
	Consumer   sarama.Consumer
	Partitions []int32
	OffMgr     sarama.OffsetManager
	Closing    *chan struct{}
	Messages   chan *sarama.ConsumerMessage
	Topic      string
	GroupName  string
}

func InitKafka(cfg KafkaConfig) (*Kafka, error) {
	var err error

	k := &Kafka{
		Topic:    cfg.Topic,
		Messages: make(chan *sarama.ConsumerMessage),

		GroupName: cfg.GroupName,
	}

	closing := make(chan struct{})
	k.Closing = &closing

	config := sarama.NewConfig()

	config.Net.SASL.Enable = false
	config.Net.SASL.User = cfg.User
	config.Net.SASL.Password = cfg.Password
	config.Net.TLS.Enable = false
	config.Net.TLS.Config = &tls.Config{InsecureSkipVerify: true}
	config.Consumer.Return.Errors = true

	k.Consumer, err = sarama.NewConsumer(cfg.BrokerList, config)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"topic":       "kafka",
			"event":       "NewConsumer",
			"kafka_topic": k.Topic,
			"brokers":     cfg.BrokerList,
		}).Error(err)
		return nil, err
	}
	k.Partitions, err = k.Consumer.Partitions(cfg.Topic)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"topic": "kafka",
			"event": "Partitions",
		}).Error(err)
		return nil, err
	}
	client, err := sarama.NewClient(cfg.BrokerList, config)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"topic": "kafka",
			"event": "NewClient",
		}).Error(err)
		return nil, err
	}
	k.OffMgr, err = sarama.NewOffsetManagerFromClient(k.GroupName, client)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"topic": "kafka",
			"event": "NewOffsetManagerFromClient",
		}).Error(err)
		return nil, err
	}
	return k, nil
}

func (s *Kafka) ConsumeAndIndex(wg *sync.WaitGroup) error {

	for _, partition := range s.Partitions {
		var err error
		var sc ConsumeManager

		sc.pm, err = s.OffMgr.ManagePartition(s.Topic, partition)
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"topic":     "kafka",
				"event":     "ManagePartition",
				"partition": partition,
			}).Error(err)
			return err
		}

		offset := sarama.OffsetNewest
		sc.pc, err = s.Consumer.ConsumePartition(s.Topic, partition, offset)
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"topic":     "kafka",
				"event":     "ConsumePartition",
				"partition": partition,
			}).Error(err)
			return err
		}

		go s.HandlePartitionConsumerErrors(sc.pc, offset, partition)

		go func(pc sarama.PartitionConsumer) {
			<-*s.Closing
			pc.AsyncClose()
		}(sc.pc)

		logrus.WithFields(logrus.Fields{
			"topic":     "kafka",
			"offset":    offset,
			"partition": partition,
		}).Info("starting partition consumer")

		wg.Add(1)

		go func() {
			defer wg.Done()
			for msg := range sc.pc.Messages() {

				s.Messages <- msg
				sc.pm.MarkOffset(msg.Offset+1, s.GroupName)

			}
		}()
	}

	return nil
}

func (s *Kafka) HandlePartitionConsumerErrors(pc sarama.PartitionConsumer, offset int64, partition int32) {
	for err := range pc.Errors() {
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"topic":     "kafka",
				"event":     "PartitionConsumerErr",
				"partition": partition,
			}).Error(err)
		}
	}
}

// fmt.Printf("Data: %08b \n", data[:4])
func printAsJSON(i interface{}) {
	b, err := json.MarshalIndent(i, "", "  ")
	if err != nil {
		log.Println(err)
	}
	fmt.Println(string(b))
}
