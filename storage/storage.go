package storage

import (
	"sync"
)

type ListStorage struct {
	mu       sync.Mutex
	commands [][]byte
}

func (s *ListStorage) ApplyCommand(command []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.commands = append(s.commands, command)
}

func (s *ListStorage) Last() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.commands) == 0 {
		return nil
	}
	last := s.commands[len(s.commands)-1]
	lastCopy := make([]byte, len(last))
	copy(lastCopy, last)
	return lastCopy
}
