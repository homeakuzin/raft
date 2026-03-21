package storage

import (
	"encoding/json"
	"fmt"
	"log"
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
	Value  string
}

func (c Command) String() string {
	if c.Action == ActionSet {
		return fmt.Sprintf("SET %s = (%d bytes)", c.Key, len(c.Value))
	} else {
		return fmt.Sprintf("DELETE %s", c.Key)
	}
}

type KVStorage struct {
	mu    sync.Mutex
	state map[string]string
}

func NewKVStorage() *KVStorage {
	return &KVStorage{state: make(map[string]string)}
}

func (s *KVStorage) Get(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	val, ok := s.state[key]
	return val, ok
}

func (s *KVStorage) ApplyCommand(command []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var cmd Command
	if err := json.Unmarshal(command, &cmd); err != nil {
		log.Printf("KVStorage: could not parse command json: %s", err.Error())
	}
	switch cmd.Action {
	case ActionSet:
		s.state[cmd.Key] = cmd.Value
	case ActionDelete:
		delete(s.state, cmd.Key)
	}
}

func (s *KVStorage) MustGet(key string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	val, ok := s.state[key]
	if !ok {
		panic("key not present in KVStorage")
	}
	return val
}

type ListStorage struct {
	mu       sync.Mutex
	commands [][]byte
}

func (s *ListStorage) ApplyCommand(command []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.commands = append(s.commands, command)
}

func (s *ListStorage) Commands() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.commands
}
