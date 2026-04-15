package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)


func main() {
	fmt.Println("Starting Peril client...")
	connectionString:= "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatal(err)
		return
	}
	defer conn.Close()
	fmt.Println("Peril game client connected to RabbitMQ!")


	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatal(err)
		return
	}

	_, queue, err:= pubsub.DeclareAndBind(conn, routing.ExchangePerilDirect, routing.PauseKey + "." + username, routing.PauseKey, pubsub.Transient)

	if err != nil {
		log.Fatalf("could not Declate and Bind the queue: %v", err)
	}
	fmt.Printf("Queue %v declared and bound!\n", queue.Name)


	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("RabbitMQ connection closed.")

}
