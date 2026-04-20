package main

import (
	"fmt"
	"log"
	"time"

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
		handlerMove(gs, publishCh),
	) 

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		routing.PauseKey+"."+gs.GetUsername(),
		routing.PauseKey,
		pubsub.Transient,
		handlerPause(gs),
	)

	err = pubsub.SubscribeJSON(
		conn,
		string(routing.ExchangePerilTopic),
		string(routing.WarRecognitionsPrefix),
		string(routing.WarRecognitionsPrefix) + ".*",
		pubsub.Durable,
		handlerWar(gs, publishCh),
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

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func (ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove) pubsub.AckType {
	return func (am gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		moveOutcome:= gs.HandleMove(am)
		switch moveOutcome {
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack 
		case gamelogic.MoveOutcomeMakeWar:
			err := pubsub.PublishJSON(ch, routing.ExchangePerilTopic, routing.WarRecognitionsPrefix + "." + gs.GetUsername(), gamelogic.RecognitionOfWar{
				Attacker: am.Player,
				Defender: gs.GetPlayerSnap(),
			})
			if err != nil {
				fmt.Printf("error: %s\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.NackDiscard
		default:
			return pubsub.NackDiscard
		}
		
	}
}

func handlerWar(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func (rw gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")
		warOutcome, winner, loser := gs.HandleWar(rw)
		message := "";
		switch warOutcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			message = winner + " won a war against " + loser
			err := publishGameLog(ch, gs.GetUsername(), message)
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeYouWon:
			message = winner + " won a war against " + loser
			err := publishGameLog(ch, gs.GetUsername(), message)
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeDraw:
			message = "A war between " + winner + " and " + loser + " resulted in a draw"
			err := publishGameLog(ch, gs.GetUsername(), message)
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		default:
			fmt.Println("Error, now able to satisfy any war outcome")
			return pubsub.NackDiscard
		}
	}
}

func publishGameLog(publishCh *amqp.Channel, username, msg string) error {
	return pubsub.PublishGob(
		publishCh,
		routing.ExchangePerilTopic,
		routing.GameLogSlug+"."+username,
		routing.GameLog{
			Username:    username,
			CurrentTime: time.Now(),
			Message:     msg,
		},
	)
}