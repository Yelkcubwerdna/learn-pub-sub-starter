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
	fmt.Println("Starting Peril client...")

	connect_str := "amqp://guest:guest@localhost:5672/"

	con, err := amqp.Dial(connect_str)
	if err != nil {
		fmt.Printf("Connection failed: %v\n", err)
		os.Exit(1)
	}
	defer con.Close()

	fmt.Println("Connected Successfully")

	pub_chan, err := con.Channel()
	if err != nil {
		fmt.Println("Unable to open channel: ", err)
		os.Exit(1)
	}

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Println("Error with username: ", err)
	}

	state := gamelogic.NewGameState(username)

	pubsub.SubscribeJSON(
		con,
		routing.ExchangePerilDirect,
		fmt.Sprintf("pause.%s", username),
		routing.PauseKey,
		pubsub.Transient,
		handlerPause(state),
	)

	pubsub.SubscribeJSON(
		con,
		routing.ExchangePerilTopic,
		fmt.Sprintf("army_moves.%s", username),
		"army_moves.*",
		pubsub.Transient,
		handlerArmyMoves(state, pub_chan),
	)

	pubsub.SubscribeJSON(
		con,
		routing.ExchangePerilTopic,
		"war",
		fmt.Sprintf("%s.%s", routing.WarRecognitionsPrefix, username),
		pubsub.Durable,
		handlerWarMessages(state, pub_chan),
	)

outer:
	for true {
		inputs := gamelogic.GetInput()

		switch inputs[0] {
		case "spawn":
			err := state.CommandSpawn(inputs)
			if err != nil {
				fmt.Println("Unable to complete spawn command: ", err)
			}

		case "move":
			mv, err := state.CommandMove(inputs)
			if err != nil {
				fmt.Println("Unable to complete move command: ", err)
			} else {
				fmt.Println("Move Command Successful")
			}
			err = pubsub.PublishJSON(
				pub_chan,
				routing.ExchangePerilTopic,
				fmt.Sprintf("army_moves.%s", username),
				mv,
			)
			if err != nil {
				fmt.Println("Couldn't publish move message: ", err)
			} else {
				fmt.Println("Move published successfully.")
			}

		case "status":
			state.CommandStatus()

		case "help":
			gamelogic.PrintClientHelp()

		case "spam":
			fmt.Println("Spamming not allowed yet!")

		case "quit":
			gamelogic.PrintQuit()
			break outer

		default:
			fmt.Println("Unknown command!")
			continue
		}
	}

	// Wait for Ctrl+C signal to exit

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	_ = <-signalChan
	fmt.Println("Program is shutting down...")
}
