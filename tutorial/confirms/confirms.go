package main

import (
	"context"
	"log"
	"os"

	"github.com/joho/godotenv"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

var _default_message_count = 50_000

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	addr := os.Getenv("RABBIT_MQ_URL")
	conn, err := amqp.Dial(addr)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.Confirm(false)
	failOnError(err, "Failed to confirm")

	confirms := make(chan amqp.Confirmation)
	ch.NotifyPublish(confirms)
	go func() {
		for confirm := range confirms {
			if confirm.Ack {
				log.Printf("Confirmed %d", confirm.DeliveryTag)
			} else {
				log.Printf("Nacked")
			}
		}
	}()

	q, err := ch.QueueDeclare("test_q", true, false, false, false, nil)
	failOnError(err, "Failed to declare a queue")

	consume(ch, q.Name)
	publish(ch, q.Name, "hello")
	publish(ch, q.Name, "world")

	var forever chan struct{}
	<-forever

}

func consume(ch *amqp.Channel, qName string) {
	msgs, err := ch.Consume(qName, "", false, false, false, false, nil)
	failOnError(err, "Failed to register a consume")

	go func() {
		for d := range msgs {
			log.Printf("received a message %s", d.Body)
			// d.Ack(false)
		}
	}()
}

func publish(ch *amqp.Channel, qName string, text string) {
	err := ch.PublishWithContext(context.Background(), "", qName, false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte(text),
	})
	failOnError(err, "Failed to publish a message")
}
