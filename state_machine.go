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

type Entry struct {
	Command Command
	Term    int
}

type StateMachine struct {
	mu          sync.Mutex
	logs        []Entry
	state       map[string][]byte
	lastApplied int
	commitIndex int
}

func NewStateMachine() *StateMachine {
	return &StateMachine{mu: sync.Mutex{}, state: make(map[string][]byte), lastApplied: -1, commitIndex: -1}
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

func (m *StateMachine) MustGet(key string) []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	val, ok := m.state[key]
	if !ok {
		panic("key not present in state machine")
	}
	return val
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

func (m *StateMachine) Get(key string) ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	val, ok := m.state[key]
	return val, ok
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
		switch log.Command.Action {
		case ActionSet:
			m.state[log.Command.Key] = log.Command.Value
		case ActionDelete:
			delete(m.state, log.Command.Key)
		}
	}
	m.lastApplied = until
}
