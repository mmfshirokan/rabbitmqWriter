package main

import (
	"context"
	"strconv"
	"sync"

	ampq "github.com/rabbitmq/amqp091-go"
	log "github.com/sirupsen/logrus"
)

const (
	key = "data"
)

var (
	wg sync.WaitGroup
)

func main() {
	conn, err := ampq.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("unable to open connect to RabbitMQ server. Error: %s", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("failed to open channel. Error: %s", err)
	}
	defer ch.Close()

	for i := 0; i < 3; i++ {
		q, err := ch.QueueDeclare(
			key+strconv.FormatInt(int64(i), 10), // name
			false,                               // durable
			false,                               // delete when unused
			false,                               // exclusive
			false,                               // no-wait
			nil,                                 // arguments
		)
		if err != nil {
			log.Fatalf("failed to declare a queue. Error: %s", err)
		}
		log.Infof("queue %v declared, with name: %v", i, q.Name)
	}

	ctx := context.Background()

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go MessageWriter(ctx, ch, strconv.FormatInt(int64(i), 10), i*1000, i*1000+1000)
	}

	wg.Wait()

}

func MessageWriter(ctx context.Context, ch *ampq.Channel, queueNumber string, lowerLimit int, uperLimit int) {
	defer wg.Done()
	for i := lowerLimit; i < uperLimit; i++ {
		err := ch.PublishWithContext(
			ctx,
			"",
			key+queueNumber,
			false,
			false,
			ampq.Publishing{
				ContentType: "text/plain",
				Body:        []byte(strconv.FormatInt(int64(i), 10)),
			},
		)
		if err != nil {
			log.Print("Failed to write message:", err)
		}
	}
}
