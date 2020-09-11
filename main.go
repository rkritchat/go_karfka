package main

import (
	"encoding/json"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/joho/godotenv"
	"go.uber.org/zap"
	"net/http"
	"os"
)

const (
	kafkaServer = "KAFKA_SERVERS"
	kafkaGroup  = "KAFKA_GROUP"
	kafkaTopic  = "KAFKA_TOPIC"
	port        = "PORT"
)

func main() {
	//load config
	_ = godotenv.Load()

	//init logs
	var cfg zap.Config
	cfg = zap.NewProductionConfig()
	cfg.OutputPaths = []string{
		"./app.log", "stdout",
	}
	logger, _ := cfg.Build()
	zap.ReplaceGlobals(logger)

	//init kafka
	producer, consumer := initKafka()
	defer producer.Close()
	defer consumer.Close()

	end := make(chan bool)
	defer func() {
		end <- true
	}()

	go consume(consumer, end)

	http.HandleFunc("/ping", ping(producer))
	zap.S().Infof("Start on port %v", os.Getenv(port))
	err := http.ListenAndServe( os.Getenv(port), nil)
	if err != nil {
		zap.S().Errorf("ListenAndServe error: %v", err)
	}
}

type Req struct {
	Test string `json:"test"`
}

func ping(producer *kafka.Producer) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		defer w.Write([]byte("test"))
		zap.S().Info("Ping coming")

		var req Req
		req.Test = "Hello kafka"
		reqJson, _ := json.Marshal(&req)

		topic := os.Getenv("KAFKA_TOPIC")
		err := producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          reqJson,
		}, nil)

		if err != nil {
			zap.S().Errorf("error while produceMsg: %v", err)
		}
	}
}

func initKafka() (*kafka.Producer, *kafka.Consumer) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": os.Getenv(kafkaServer),
	})
	if err != nil {
		panic(err)
	}

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": os.Getenv(kafkaServer),
		"group.id":          os.Getenv(kafkaGroup),
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		panic(err)
	}

	return producer, consumer
}

func consume(consumer *kafka.Consumer, end chan bool) {
	//init consumer
	_ = consumer.SubscribeTopics([]string{os.Getenv(kafkaTopic)}, nil)
	run := true
	for run {
		select {
		case <-end:
			run = false
		default:
			msg, err := consumer.ReadMessage(-1)
			if err == nil {
				zap.S().Info("Message coming")
			} else {
				// The client will automatically try to recover from all errors.
				zap.S().Errorf("Consumer error: %v (%v)", err, msg)
			}
		}
	}
}
