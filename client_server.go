package main

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
)

type RaftState struct {
	State State
	Term  int
}

func (n *Node) clientHandlerRaft(w http.ResponseWriter, r *http.Request) {
	state := RaftState{n.getState(), n.getCurrentTerm()}
	stateBytes, err := json.Marshal(&state)
	if err != nil {
		log.Printf("Could not marshal RaftState: %s", err.Error())
		w.WriteHeader(500)
	}
	w.Header().Add("Content-Type", "application/json")
	w.Write(stateBytes)
}

type updateRequest struct {
	cmd      Command
	response chan int
}

func (n *Node) clientHandlerUpdate(w http.ResponseWriter, r *http.Request) {
	value, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("clientHandlerUpdate: could not read body: %s", err.Error())
		w.WriteHeader(500)
		return
	}
	if len(value) == 0 {
		w.WriteHeader(400)
		w.Write([]byte("Expected non-empty request body"))
		return
	}
	if n.getState() != Leader {
		w.WriteHeader(422)
		w.Write([]byte("Node is not a leader"))
		return
	}
	ur := updateRequest{
		Command{
			Action: ActionSet,
			Key:    r.PathValue("key"),
			Value:  value,
		},
		make(chan int),
	}
	n.heartbeatTimer.Reset(heartbeatPeriod)
	n.stateUpdateRequestCh <- ur
	response := <-ur.response
	if response != 200 {
		w.WriteHeader(response)
	}
}

func (n *Node) clientHandlerDelete(w http.ResponseWriter, r *http.Request) {
	if n.getState() != Leader {
		w.WriteHeader(422)
		w.Write([]byte("Node is not a leader"))
		return
	}
}

func (n *Node) RunClientServer(addr string) error {
	handler := http.NewServeMux()
	handler.HandleFunc("POST /{key}", n.clientHandlerUpdate)
	handler.HandleFunc("DELETE /{key}", n.clientHandlerDelete)
	handler.HandleFunc("GET /raft", n.clientHandlerRaft)
	log.Printf("Running HTTP client server at %v", addr)
	n.clientServer = &http.Server{Addr: addr, Handler: handler}
	err := n.clientServer.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		err = nil
	}
	return nil
}
