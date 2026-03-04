package main

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
)

type RaftState struct {
	State State
	Term  int
}

func (n *Node) handlerRaftState(w http.ResponseWriter, r *http.Request) {
	state := RaftState{n.getState(), n.getCurrentTerm()}
	stateBytes, err := json.Marshal(&state)
	if err != nil {
		log.Printf("Could not marshal RaftState: %s", err.Error())
		w.WriteHeader(500)
	}
	w.Header().Add("Content-Type", "application/json")
	w.Write(stateBytes)
}

func (n *Node) RunClientServer(addr string) error {
	handler := http.NewServeMux()
	// handler.HandleFunc("POST /{key}", n.handlerRequestVote)
	// handler.HandleFunc("DELETE /{key}", n.handlerRequestVote)
	handler.HandleFunc("GET /raft", n.handlerRaftState)
	log.Printf("Running HTTP client server at %v", addr)
	n.clientServer = &http.Server{Addr: addr, Handler: handler}
	err := n.clientServer.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		err = nil
	}
	return nil
}
