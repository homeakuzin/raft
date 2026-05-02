package raft

type EventHandler struct {
	f      func(*EventHandler, any)
	active bool
}

func (h *EventHandler) Stop() {
	h.active = false
}

type EventBecomeCandidate struct {
}

type EventBecomeFollower struct {
}

type EventBecomeLeader struct {
}

type EventCommit struct {
	NewCommitIndex int
}

type EventTerm struct {
	NewTerm int
}
