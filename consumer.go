package main

import (
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func StartConsumer(topics []string, run *bool) error {

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "foo",
		"auto.offset.reset": "earliest"})

	if err != nil {
		return err
	}

	defer consumer.Close()

	err = consumer.SubscribeTopics(topics, nil)

	if err != nil {
		return err
	}

	for *run {
		ev := consumer.Poll(100)
		switch e := ev.(type) {
		case *kafka.Message:
			fmt.Printf("Successfully consumed record to topic %s partition [%d] @ offset %v\n",
						*e.TopicPartition.Topic, e.TopicPartition.Partition, e.TopicPartition.Offset)
		case kafka.Error:
			fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
			*run = false
		default:
			fmt.Printf("Ignored %v\n", e)
		}
	}

	return nil
}
