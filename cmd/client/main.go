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

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Println("Error with username: ", err)
	}

	_, _, err = pubsub.DeclareAndBind(con,
		routing.ExchangePerilDirect,
		fmt.Sprintf("%s.%s", routing.PauseKey, username),
		routing.PauseKey,
		pubsub.Transient,
	)
	if err != nil {
		fmt.Println("Error during declare and bind: ", err)
		os.Exit(1)
	}

	state := gamelogic.NewGameState(username)

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
			_, err := state.CommandMove(inputs)
			if err != nil {
				fmt.Println("Unable to complete move command: ", err)
			} else {
				fmt.Println("Move Command Successful")
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
