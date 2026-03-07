package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"raft"
)

type agent struct {
	node       *raft.Node
	httpServer *http.Server
}

func newAgent(node *raft.Node) agent {
	a := agent{node: node}
	http.HandleFunc("GET /status", a.status)
	http.HandleFunc("POST /command", a.command)
	return a
}

func (a agent) shutdown(ctx context.Context) {
	a.httpServer.Shutdown(ctx)
}

func (a agent) run(addr string) error {
	a.httpServer = &http.Server{Addr: addr}
	err := a.httpServer.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		err = nil
	}
	return nil
}

func (a agent) command(w http.ResponseWriter, r *http.Request) {
	command, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("could not read /command body: %s", err.Error())
		w.WriteHeader(500)
		return
	}
	a.node.ClientCommand(r.Context(), command)
}

func (a agent) status(w http.ResponseWriter, r *http.Request) {
	status := nodeStatus{
		int(a.node.Id),
		a.node.State().String(),
		a.node.CurrentTerm(),
		a.node.StateMachine.CommitIndex(),
	}
	jsonBytes, err := json.Marshal(&status)
	if err != nil {
		log.Printf("marshal %+v: %s", status, err.Error())
		w.WriteHeader(500)
		return
	}
	w.Header().Add("Content-Type", "application/json")
	w.Write(jsonBytes)
}
