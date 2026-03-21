package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"

	"github.com/homeakuzin/raft"
	"github.com/homeakuzin/raft/storage"
)

var flagNodes = flag.String("nodes", "", "id:host:port joined by semicolon. Example:\n\t0:0.0.0.1:1234;1:0.0.0.2:2345;2:0.0.0.3:3456")
var flagNodeId = flag.Int("id", int(raft.EmptyId), "Current node ID")
var flagMetricsAddr = flag.String("metricsaddr", "", "Address to serve prometheus metrics")

type showcaseNode struct {
	id        raft.NodeId
	addr      string
	agentAddr string
}

func main() {
	log.SetFlags(log.Lmicroseconds | log.Ldate)
	log.SetOutput(os.Stdout)
	flag.Parse()

	nodes, err := parseNodesFlag(*flagNodes)
	if err != nil {
		log.Fatal(err.Error())
	}

	if *flagNodeId != int(raft.EmptyId) {
		runANode(nodes)
		return
	}
	runDashboard("localhost:3456", nodes)
}

func parseNodesFlag(nodesStr string) ([]showcaseNode, error) {
	nodes := make([]showcaseNode, 0, 3)
	nodeParts := strings.Split(nodesStr, ";")
	for i := range nodeParts {
		idHostAndPort := strings.Split(nodeParts[i], ":")
		if len(idHostAndPort) != 5 {
			flag.Usage()
			log.Fatal("Incorrect -nodes usage")
		}
		nodeIdInt, err := strconv.Atoi(idHostAndPort[0])
		if err != nil {
			log.Fatal("Incorrect -nodes usage")
		}
		nodes = append(nodes, showcaseNode{
			raft.NodeId(nodeIdInt),
			idHostAndPort[1] + ":" + idHostAndPort[2],
			idHostAndPort[3] + ":" + idHostAndPort[4],
		})
	}
	if len(nodes) < 3 {
		return nil, errors.New("We work with 3 nodes minimum")
	}
	return nodes, nil
}

func runANode(nodes []showcaseNode) {
	nodeId := raft.NodeId(*flagNodeId)
	log.SetPrefix(fmt.Sprintf("[%s] ", nodeId))

	agentAddr := ""
	peers := make(map[raft.NodeId]string)
	for _, node := range nodes {
		peers[node.id] = node.addr
		if node.id == nodeId {
			agentAddr = node.agentAddr
		}
	}

	if agentAddr == "" {
		log.Fatalf("No node with id %d", *flagNodeId)
	}

	node := raft.NewNode(nodeId, peers, raft.HTTPTransport(nodeId, peers), &storage.ListStorage{})

	agent := newAgent(node)
	go func() {
		log.Printf("running agent at http://%s", agentAddr)
		if err := agent.run(agentAddr); err != nil {
			log.Printf("could not run agent: %s", err.Error())
		}
	}()

	if *flagMetricsAddr != "" {
		// reg := prometheus.NewRegistry()
		// reg.MustRegister(
		// 	collectors.NewGoCollector(),
		// 	collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		// )
		// // TODO broadcastTime/electionTimeout/MTBF metrics
		// http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
		// log.Printf("exposing prometheus metrics at %s", *flagMetricsAddr)
		// go http.ListenAndServe(*flagMetricsAddr, nil)
	}

	go node.Run()

	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt)
	<-sigint
	log.Print("SIGINT shutdown")
	agent.shutdown(context.Background())
	node.Shutdown(context.Background())
}
