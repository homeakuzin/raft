package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"raft"
	"raft/storage"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var flagVerbose = flag.Bool("v", false, "enables verbose output")

func main() {
	log.SetFlags(log.Lmicroseconds)
	log.SetOutput(os.Stdout)

	flagNodes := flag.String("nodes", "", "id:host:port joined by semicolon. Example:\n\t0:0.0.0.1:1234;1:0.0.0.2:2345;2:0.0.0.3:3456")
	flagNodeId := flag.Int("id", int(raft.EmptyId), "Current node ID")
	flagClientAddr := flag.String("clientaddr", "", "Address to serve HTTP clients")
	flagMetricsAddr := flag.String("metricsaddr", "", "Address to serve prometheus metrics")
	flagReport := flag.Bool("report", false, "Whether to report node state every second")
	flag.Parse()

	nodeId := raft.NodeId(*flagNodeId)
	log.SetPrefix(fmt.Sprintf("[%s] ", nodeId))

	nodes := make(map[raft.NodeId]string)
	nodeParts := strings.Split(*flagNodes, ";")
	for i := range nodeParts {
		idHostAndPort := strings.Split(nodeParts[i], ":")
		if len(idHostAndPort) != 3 {
			flag.Usage()
			log.Fatal("Incorrect -nodes usage")
		}
		nodeIdInt, err := strconv.Atoi(idHostAndPort[0])
		if err != nil {
			log.Fatal("Incorrect -nodes usage")
		}
		nodes[raft.NodeId(nodeIdInt)] = idHostAndPort[1] + ":" + idHostAndPort[2]
	}

	if len(nodes) < 3 {
		log.Fatalf("We work with 3 nodes minimum")
	}

	if _, ok := nodes[raft.NodeId(*flagNodeId)]; !ok {
		log.Fatalf("No node with id %d", *flagNodeId)
	}

	if *flagMetricsAddr != "" {
		reg := prometheus.NewRegistry()
		reg.MustRegister(
			collectors.NewGoCollector(),
			collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		)
		// TODO broadcastTime/electionTimeout/MTBF metrics
		http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
		log.Printf("exposing prometheus metrics at %s", *flagMetricsAddr)
		go http.ListenAndServe(*flagMetricsAddr, nil)
	}

	kvStorage := storage.NewKVStorage()
	node := raft.NewNode(nodeId, nodes, kvStorage)
	if *flagVerbose {
		node.Verbose()
	}
	if *flagReport {
		node.StartReporting()
	}
	if *flagClientAddr != "" {
		go func() {
			if err := RunClientServer(*flagClientAddr, node, kvStorage); err != nil {
				log.Printf("Could not run client HTTP server: %s", err.Error())
			}
		}()
	}
	node.Run()
}
