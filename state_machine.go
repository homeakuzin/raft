package main

import (
	"fmt"
	"sync"
)

type Action int

const (
	ActionSet    Action = iota
	ActionDelete        = iota
)

type Command struct {
	Action Action
	Key    string
	Value  []byte
}

func (c Command) String() string {
	if c.Action == ActionSet {
		return fmt.Sprintf("SET %s = (%d bytes)", c.Key, len(c.Value))
	} else {
		return fmt.Sprintf("DELETE %s", c.Key)
	}
}

type StateMachine struct {
	mu        sync.Mutex
	State     map[string][]byte
	Logs      []Command
	LastIndex int
}

func NewStateMachine() *StateMachine {
	return &StateMachine{mu: sync.Mutex{}, State: make(map[string][]byte)}
}

func (m *StateMachine) Apply(command Command) {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch command.Action {
	case ActionSet:
		m.State[command.Key] = command.Value
	case ActionDelete:
		delete(m.State, command.Key)
	}
}

func (m *StateMachine) Restore() {
	m.State = make(map[string][]byte)
	for i := range m.Logs {
		m.Apply(m.Logs[i])
	}
}
