package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")

	connect_str := "amqp://guest:guest@localhost:5672/"

	con, err := amqp.Dial(connect_str)
	if err != nil {
		fmt.Printf("Connection failed: %v\n", err)
		os.Exit(1)
	}
	defer con.Close()

	fmt.Println("Connected Successfully")

	_, _, err = pubsub.DeclareAndBind(con, routing.ExchangePerilTopic, routing.GameLogSlug, fmt.Sprintf("%s.*", routing.GameLogSlug), pubsub.Durable)
	if err != nil {
		fmt.Print("Unable to declare and bind: ", err)
		os.Exit(1)
	}

	rabbit_chan, err := con.Channel()
	if err != nil {
		fmt.Print("Failed to open channel: ", err)
		os.Exit(1)
	}

	gamelogic.PrintServerHelp()

outer:
	for true {
		inputs := gamelogic.GetInput()

		if len(inputs) == 0 {
			continue
		}

		switch inputs[0] {
		case "pause":
			fmt.Println("Sending pause message...")
			pubsub.PublishJSON(rabbit_chan, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
		case "resume":
			fmt.Println("Sending resume message...")
			pubsub.PublishJSON(rabbit_chan, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false})
		case "quit":
			fmt.Println("Exiting...")
			break outer
		default:
			fmt.Println("Unknown command!")
		}
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	_ = <-signalChan
	fmt.Println("Program is shutting down...")
}
