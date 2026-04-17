package main

import (
	"fmt"
	"log"

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

	publishCh, err := conn.Channel();

	if err != nil {
		log.Fatalf("could not create channel: %v", err)
	}

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatal(err)
		return
	}

	gs := gamelogic.NewGameState(username)

	err = pubsub.SubscribeJSON(
		conn,
		string(routing.ExchangePerilTopic),
		string(routing.ArmyMovesPrefix) + "." + username,
		string(routing.ArmyMovesPrefix) + ".*",
		pubsub.Transient,
		handlerMove(gs),
	) 

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		routing.PauseKey+"."+gs.GetUsername(),
		routing.PauseKey,
		pubsub.Transient,
		handlerPause(gs),
	)
	if err != nil {
		log.Fatalf("could not subscribe the queue: %v", err)
	}

	for {
		words := gamelogic.GetInput()
		switch words[0] {
		case "spawn":
			err:= gs.CommandSpawn(words)
			if err != nil {
				log.Printf("could not command spawn: %v", err)
			}
		case "move":
			mv, err:= gs.CommandMove(words)
			if err != nil {
				log.Printf("could not command move: %v", err)
			}
			err = pubsub.PublishJSON(
				publishCh,
				string(routing.ExchangePerilTopic),
				string(routing.ArmyMovesPrefix) + "." + username,
				mv,
			)
			if err != nil {
				fmt.Printf("error: %s\n", err)
				continue
			}
			fmt.Printf("Moved %v units to %s\n", len(mv.Units), mv.ToLocation)
		case "status":
			gs.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		default :
			fmt.Println("not allowed command")		
		}		
	}
}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) {
	return func (ps routing.PlayingState)  {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
	}
}

func handlerMove(gs *gamelogic.GameState) func(gamelogic.ArmyMove) {
	return func (am gamelogic.ArmyMove)  {
		defer fmt.Print("> ")
		gs.HandleMove(am)
	}
}
