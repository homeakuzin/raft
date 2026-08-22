package main_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	. "github.com/homeakuzin/raft"
)

type link struct {
	from NodeId
	to   NodeId
}

type networkConditions struct {
	t        testing.TB
	mu       sync.Mutex
	cutLinks map[link]bool
	latency  map[link]time.Duration
}

const (
	networkConditionsLogColor = "\x1b[31m"
	networkConditionsLogReset = "\x1b[0m"
)

func newNetworkConditions(t testing.TB) *networkConditions {
	return &networkConditions{
		t:        t,
		cutLinks: make(map[link]bool),
		latency:  make(map[link]time.Duration),
	}
}

func (n *networkConditions) Cut(from, to NodeId) {
	n.mu.Lock()
	n.cutLinks[link{from: from, to: to}] = true
	n.cutLinks[link{from: to, to: from}] = true
	n.mu.Unlock()

	n.logf("network conditions: cut link %d <-> %d", from, to)
}

func (n *networkConditions) Heal(from, to NodeId) {
	n.mu.Lock()
	delete(n.cutLinks, link{from: from, to: to})
	delete(n.cutLinks, link{from: to, to: from})
	n.mu.Unlock()

	n.logf("network conditions: heal link %d <-> %d", from, to)
}

func (n *networkConditions) Latency(from, to NodeId, d time.Duration) {
	n.mu.Lock()
	n.latency[link{from: from, to: to}] = d
	n.latency[link{from: to, to: from}] = d
	n.mu.Unlock()

	n.logf("network conditions: set latency %s on link %d <-> %d", d, from, to)
}

func (n *networkConditions) ClearLatency(from, to NodeId) {
	n.mu.Lock()
	delete(n.latency, link{from: from, to: to})
	delete(n.latency, link{from: to, to: from})
	n.mu.Unlock()

	n.logf("network conditions: clear latency on link %d <-> %d", from, to)
}

func (n *networkConditions) beforeSend(ctx context.Context, from, to NodeId) error {
	if err := n.checkCut(from, to); err != nil {
		return err
	}

	delay := n.linkLatency(from, to)
	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}

	return n.checkCut(from, to)
}

func (n *networkConditions) checkCut(from, to NodeId) error {
	n.mu.Lock()
	cut := n.cutLinks[link{from: from, to: to}]
	n.mu.Unlock()
	if cut {
		return fmt.Errorf("network link cut: %d -> %d", from, to)
	}
	return nil
}

func (n *networkConditions) linkLatency(from, to NodeId) time.Duration {
	n.mu.Lock()
	defer n.mu.Unlock()

	return n.latency[link{from: from, to: to}]
}

func (n *networkConditions) logf(format string, args ...any) {
	if n.t == nil {
		return
	}
	n.t.Helper()
	n.t.Logf(networkConditionsLogColor+format+networkConditionsLogReset, args...)
}

type conditionedTransport struct {
	id   NodeId
	base Transport
	net  *networkConditions
}

func withNetworkConditions(id NodeId, base Transport, net *networkConditions) Transport {
	return &conditionedTransport{id: id, base: base, net: net}
}

func (t *conditionedTransport) Serve(ctx context.Context, requestVoteCallback func(args RequestVoteArgs, replyCh chan<- RequestVoteReply), appendEntriesCallback func(args AppendEntriesArgs, replyCh chan<- AppendEntriesReply)) error {
	return t.base.Serve(ctx, requestVoteCallback, appendEntriesCallback)
}

func (t *conditionedTransport) Shutdown(ctx context.Context) {
	t.base.Shutdown(ctx)
}

func (t *conditionedTransport) RequestVote(ctx context.Context, to NodeId, data RequestVoteArgs) (RequestVoteReply, error) {
	if err := t.net.beforeSend(ctx, t.id, to); err != nil {
		return RequestVoteReply{}, err
	}
	return t.base.RequestVote(ctx, to, data)
}

func (t *conditionedTransport) AppendEntries(ctx context.Context, to NodeId, data AppendEntriesArgs) (AppendEntriesReply, error) {
	if err := t.net.beforeSend(ctx, t.id, to); err != nil {
		return AppendEntriesReply{}, err
	}
	return t.base.AppendEntries(ctx, to, data)
}
