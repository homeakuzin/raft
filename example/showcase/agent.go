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

type nodeCommand struct {
	Node    string
	Command string
}

func newAgent(node *raft.Node) agent {
	a := agent{node: node}
	http.HandleFunc("/apply", a.applyCommandHandler)
	http.HandleFunc("/status", a.statusHandler)
	http.HandleFunc("/command", a.commandHandler)
	return a
}

func (a agent) onCommand(ctx context.Context, command nodeCommand) {
	if command.Command == "stop" {
		a.node.Shutdown(ctx)
	} else if command.Command == "start" {
		go a.node.Run()
	}
}

func (a agent) shutdown(ctx context.Context) {
	log.Print("shutting down agent")
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

func (a agent) commandHandler(w http.ResponseWriter, r *http.Request) {
	log.Printf("/command")
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("/command: %s", err.Error())
		w.WriteHeader(500)
		return
	}
	command := nodeCommand{}
	if err := json.Unmarshal(body, &command); err != nil {
		log.Printf("/command: %s", err.Error())
		w.WriteHeader(500)
		return
	}
	a.onCommand(r.Context(), command)
}

func (a agent) applyCommandHandler(w http.ResponseWriter, r *http.Request) {
	log.Printf("/apply")
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("/apply: %s", err.Error())
		w.WriteHeader(500)
		return
	}
	if a.node.State() == raft.Leader && len(body) > 0 {
		a.node.ClientCommand(r.Context(), body)
	}
}

func (a agent) statusHandler(w http.ResponseWriter, r *http.Request) {
	status := nodeStatus{
		int(a.node.Id),
		a.node.Id.String(),
		a.node.State().String(),
		a.node.CurrentTerm(),
		a.node.StateMachine.CommitIndex(),
		a.node.StateMachine.Len() - 1,
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
