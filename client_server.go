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

type ClientServer struct {
	storage *KVStorage
	node    *Node
}

func (s ClientServer) clientHandlerRaft(w http.ResponseWriter, r *http.Request) {
	state := RaftState{s.node.getState(), s.node.getCurrentTerm()}
	stateBytes, err := json.Marshal(&state)
	if err != nil {
		log.Printf("Could not marshal RaftState: %s", err.Error())
		w.WriteHeader(500)
	}
	w.Header().Add("Content-Type", "application/json")
	w.Write(stateBytes)
}

func (s ClientServer) clientHandlerGet(w http.ResponseWriter, r *http.Request) {
	value, ok := s.storage.Get(r.PathValue("key"))
	if ok {
		w.Write([]byte(value))
	} else {
		w.WriteHeader(404)
	}
}

func (s ClientServer) clientHandlerUpdate(w http.ResponseWriter, r *http.Request) {
	value, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("clientHandlerUpdate: could not read body: %s", err.Error())
		w.WriteHeader(500)
		return
	}
	if len(value) == 0 {
		w.WriteHeader(400)
		log.Printf("clientHandlerUpdate: got empty request body")
		w.Write([]byte("Expected non-empty request body"))
		return
	}
	if s.node.getState() != Leader {
		w.WriteHeader(422)
		log.Printf("clientHandlerUpdate: non-leader node")
		w.Write([]byte("Node is not a leader"))
		return
	}
	cmd := Command{
		Action: ActionSet,
		Key:    r.PathValue("key"),
		Value:  string(value),
	}
	cmdJson, err := json.Marshal(&cmd)
	if err != nil {
		log.Printf("clientHandlerUpdate: Marshal Command: %s", err.Error())
		w.WriteHeader(500)
		return
	}
	ur := updateRequest{
		cmdJson,
		make(chan int),
	}
	select {
	case s.node.stateUpdateRequestCh <- ur:
		<-ur.commited
		w.WriteHeader(200)
	case <-r.Context().Done():
	}
}

func (s ClientServer) clientHandlerDelete(w http.ResponseWriter, r *http.Request) {
	if s.node.getState() != Leader {
		w.WriteHeader(422)
		w.Write([]byte("Node is not a leader"))
		log.Printf("clientHandlerUpdate: non-leader node")
		return
	}
}

func RunClientServer(addr string, node *Node, storage *KVStorage) error {
	kvServer := ClientServer{storage, node}
	handler := http.NewServeMux()
	handler.HandleFunc("GET /{key}", kvServer.clientHandlerGet)
	handler.HandleFunc("POST /{key}", kvServer.clientHandlerUpdate)
	handler.HandleFunc("DELETE /{key}", kvServer.clientHandlerDelete)
	handler.HandleFunc("GET /raft", kvServer.clientHandlerRaft)
	log.Printf("Running HTTP client server at %v", addr)
	server := &http.Server{Addr: addr, Handler: handler}
	err := server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		err = nil
	}
	return nil
}
