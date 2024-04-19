package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/IBM/sarama"
)

func main() {
	broker := os.Getenv("KAFKA_BROKER")
	topic := os.Getenv("KAFKA_TOPIC")
	username := os.Getenv("KAFKA_USERNAME")
	password := os.Getenv("KAFKA_PASSWORD")

	// https://docs.digitalocean.com/products/databases/kafka/how-to/connect/
	config := sarama.NewConfig()
	config.Metadata.Full = true
	config.ClientID = "sample-consumer-client"
	config.Producer.Return.Successes = true

	config.Net.SASL.Enable = true
	config.Net.SASL.User = username
	config.Net.SASL.Password = password
	config.Net.SASL.Handshake = true
	config.Net.SASL.Mechanism = sarama.SASLTypePlaintext

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM([]byte(os.Getenv("KAFKA_CA_CERT")))
	tlsConfig := &tls.Config{
		RootCAs: caCertPool,
	}
	config.Net.TLS.Enable = true
	config.Net.TLS.Config = tlsConfig

	brokers := []string{broker}
	consumerClient, err := sarama.NewConsumerGroup(brokers, "sample-group", config)
	if err != nil {
		log.Fatalf("Error creating new consumer group: %v", err)
	}
	defer consumerClient.Close()

	wg := &sync.WaitGroup{}
	wg.Add(1)

	log.Println("Starting consumer")
	ctx, cancel := context.WithCancel(context.Background())

	if err := runConsumer(ctx, consumerClient, topic, wg); err != nil {
		log.Fatalf("Error consuming messages: %v", err)
	}

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-ctx.Done():
		log.Println("terminating: context cancelled")
	case <-sigterm:
		log.Println("terminating: via signal")
	}
	cancel()
	wg.Wait()
}

func runConsumer(ctx context.Context, consumerClient sarama.ConsumerGroup, topic string, wg *sync.WaitGroup) error {
	kafkaConsumer := &KafkaConsumer{ready: make(chan bool)}

	go func() {
		defer wg.Done()
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := consumerClient.Consume(ctx, []string{topic}, kafkaConsumer); err != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					return
				}
				log.Panicf("Error from consumer: %v", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			kafkaConsumer.ready = make(chan bool)
		}
	}()

	<-kafkaConsumer.ready // Await till the consumer has been set up
	log.Println("Sarama consumer up and running!...")

	return nil
}

type KafkaConsumer struct {
	ready chan bool
}

func (consumer *KafkaConsumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

func (consumer *KafkaConsumer) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (consumer *KafkaConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				log.Printf("message channel was closed")
				return nil
			}
			log.Printf("Message consumed: value = %s, timestamp = %v, topic = %s, partition = %d, offset = %d\n", string(message.Value), message.Timestamp, message.Topic, message.Partition, message.Offset)
			session.MarkMessage(message, "")
		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/IBM/sarama/issues/1192
		case <-session.Context().Done():
			return nil
		}
	}
}
