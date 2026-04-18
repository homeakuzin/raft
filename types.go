package raft

import (
	"encoding/json"
	"fmt"
)

type RequestVote struct {
	Term         int
	CandidateId  NodeId
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteResult struct {
	Term        int
	VoteGranted bool
}

type AppendEntries struct {
	Term         int
	LeaderId     NodeId
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
	LogIndex     int
}

type AppendEntriesResult struct {
	Term    int
	Success bool
}

type NodeId int

const EmptyId NodeId = -1

func (id NodeId) String() string {
	if id == EmptyId {
		return "None"
	}
	return fmt.Sprintf("node-%d", id)
}

type State int

func (s State) MarshalJSON() ([]byte, error) {
	return []byte(`"` + s.String() + `"`), nil
}

func (s *State) UnmarshalJSON(p []byte) error {
	if string(p) == `"Follower"` {
		*s = Follower
		return nil
	}
	if string(p) == `"Candidate"` {
		*s = Candidate
		return nil
	}
	if string(p) == `"Leader"` {
		*s = Leader
		return nil
	}
	if string(p) == `"Dead"` {
		*s = Dead
		return nil
	}
	intvalue := int(*s)
	err := json.Unmarshal(p, &intvalue)
	*s = State(intvalue)
	return err
}

func (s State) String() string {
	if s == Follower {
		return "Follower"
	}
	if s == Candidate {
		return "Candidate"
	}
	if s == Leader {
		return "Leader"
	}
	if s == Dead {
		return "Dead"
	}
	return "UNKNOWN"
}

const (
	Follower  State = iota
	Candidate State = iota
	Leader    State = iota
	Dead      State = iota
)
