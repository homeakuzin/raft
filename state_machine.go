package raft

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

type Entry struct {
	Command []byte
	Term    int
}

type StateStorage interface {
	ApplyCommand(command []byte)
	Get(key string) (string, bool) // TODO remove after completely separating storage and node
}

type StateMachine struct {
	mu          sync.Mutex
	logs        []Entry
	storage     StateStorage
	lastApplied int
	commitIndex int
}

func NewStateMachine(storage StateStorage) *StateMachine {
	return &StateMachine{storage: storage, mu: sync.Mutex{}, lastApplied: -1, commitIndex: -1}
}

func (m *StateMachine) LastApplied() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastApplied
}

func (m *StateMachine) SetCommitIndex(value int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.commitIndex = value
}

func (m *StateMachine) CommitIndex() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.commitIndex
}

func (m *StateMachine) Last() (Entry, int, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.logs) == 0 {
		return Entry{}, 0, false
	}
	idx := len(m.logs) - 1
	return m.logs[idx], idx, true
}
func (m *StateMachine) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.logs)
}

func (m *StateMachine) At(index int) (Entry, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if index > len(m.logs)-1 {
		return Entry{}, false
	}
	return m.logs[index], true
}

func (m *StateMachine) MustAt(index int) Entry {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.logs[index]
}

func (m *StateMachine) DeleteFrom(from int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logs = m.logs[from:]
}

func (m *StateMachine) NextEntriesForFollower(from int) []Entry {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.logs[from:]
}

func (m *StateMachine) AppendLogs(logs ...Entry) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range logs {
		m.logs = append(m.logs, logs[i])
	}
	return len(m.logs)
}

func (m *StateMachine) Apply(until int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := m.lastApplied + 1; i <= until; i++ {
		log := m.logs[i]
		m.storage.ApplyCommand(log.Command)
	}
	m.lastApplied = until
}
