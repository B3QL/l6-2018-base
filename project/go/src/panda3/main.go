package main

import (
	"os"
	"fmt"
	"sort"
	"time"
	"math/rand"
	"strconv"
	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/AsynkronIT/protoactor-go/router"
)

type InitMsg struct{
	ActorsNumber, PartsNumber int
	Data []int
}

type SortMsg struct{ Data []int }
type SortingActor struct{}

func (state *SortingActor) Receive(context actor.Context) {
	switch msg := context.Message().(type) {
	case *SortMsg:
		fmt.Printf("[MINION %s] Received data to sort %v\n", context.Self(), msg.Data)
		sort.Ints(msg.Data)
		fmt.Printf("[MINION %s] Sending sorted response to master\n", context.Self())
		context.Respond(msg)
	}
}

type SupervisorActor struct{
	router *actor.PID
	sender *actor.PID
	msgCounter int
	lastSortMsg *SortMsg
}

func (state *SupervisorActor) Receive(context actor.Context) {
	switch msg := context.Message().(type) {
	case *InitMsg:
		fmt.Printf("[MASTER] Spawning %d actors\n", msg.ActorsNumber)
		props := router.NewRoundRobinPool(msg.ActorsNumber).WithProducer(func() actor.Actor { return &SortingActor{} })
		state.router = actor.Spawn(props)
		fmt.Printf("[MASTER] Sending data to actors\n")
		dataSize := len(msg.Data) / msg.PartsNumber
		state.msgCounter = msg.PartsNumber
		state.sender = context.Sender()
		for i := 0; i < msg.PartsNumber; i++ {
			start := i * dataSize
			end := start + dataSize
			fmt.Printf("[MASTER] Slice from %d to %d\n", start, end)
			state.router.Request(&SortMsg{msg.Data[start:end]}, context.Self())
		}
	case *SortMsg:
		state.msgCounter--
		fmt.Printf("[MASTER][Waiting for %d extra] Received sorted data %v\n", state.msgCounter, msg.Data)
		if state.msgCounter == 0 {
			fmt.Printf("[MASTER] Sending final result %v\n", msg.Data)
			context.Tell(state.sender, msg.Data)
		}
		state.lastSortMsg = msg
		context.PushBehavior(state.ReceiveSecondMsg)
	}
}

func (state *SupervisorActor) ReceiveSecondMsg(context actor.Context) {
	state.msgCounter--
	switch msg := context.Message().(type) {
	case *SortMsg:
		fmt.Printf("[MASTER] Merging presored data\n")
		mergedData := append(state.lastSortMsg.Data, msg.Data...)
		state.lastSortMsg = nil
		fmt.Printf("[MASTER] Sending merged data to actors\n")
		state.msgCounter++
		state.router.Request(&SortMsg{mergedData}, context.Self())
		context.PopBehavior()
	}
}

func GenerateRandomIntegers(size int) []int {
	data := make([]int, size)
	for i := 0; i<size; i++ {
		data[i] = rand.Intn(size*2)
	}
	return data
}

func ArrayToInt(str []string) []int {
	data := make([]int, len(str))
	for i, numberString := range str {
		number, _ := strconv.Atoi(numberString)
		data[i] = number
	}
	return data
}

func main() {
	props := actor.FromProducer(func() actor.Actor { return &SupervisorActor{} })
	pid := actor.Spawn(props)
	args := ArrayToInt(os.Args[1:])
	timeout, _ := time.ParseDuration("10s")
	future := pid.RequestFuture(&InitMsg{args[0], args[1], GenerateRandomIntegers(args[2])}, timeout)
	result, _ := future.Result()
	fmt.Printf("Result: %v\n", result)
}
