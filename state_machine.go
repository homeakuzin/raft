package raft

import (
	"sync"
)

type Entry struct {
	Command []byte
	Term    int
}

type StateStorage interface {
	ApplyCommand(command []byte)
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
	m.logs = m.logs[:from]
}

func (m *StateMachine) NextEntriesForFollower(from int) []Entry {
	m.mu.Lock()
	defer m.mu.Unlock()
	if from < 0 {
		return nil
	}
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
	for i := m.lastApplied + 1; i <= until && i < len(m.logs); i++ {
		log := m.logs[i]
		m.storage.ApplyCommand(log.Command)
	}
	m.lastApplied = until
}
