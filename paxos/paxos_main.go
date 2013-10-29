package main

import "github.com/pycq2002/paxos/logic"
import "runtime"

var g_messanger logic.Messanger

func main() {
	//To use all CPU
	runtime.GOMAXPROCS(runtime.NumCPU())
	g_messanger.Init()
	proposer1 := new(logic.Proposer)
	proposer1.Init(&g_messanger, "proposer1")
	proposer2 := new(logic.Proposer)
	proposer2.Init(&g_messanger, "proposer2")
	accepter1 := new(logic.Accepter)
	accepter1.Init(&g_messanger, "accepter1")
	accepter2 := new(logic.Accepter)
	accepter2.Init(&g_messanger, "accepter2")
	accepter3 := new(logic.Accepter)
	accepter3.Init(&g_messanger, "accepter3")
	accepter4 := new(logic.Accepter)
	accepter4.Init(&g_messanger, "accepter4")

	learner1 := new(logic.Learner)
	learner1.Init(&g_messanger, "learner1")
	go proposer1.Begin_propose_loop(10)
	go proposer2.Begin_propose_loop(20)
	runtime.Goexit()
}
