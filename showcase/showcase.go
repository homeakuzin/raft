package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"slices"
	"sync"

	"github.com/gorilla/websocket"
)

type showcaseHandler struct {
	addr     string
	nodes    []showcaseNode
	upgrader websocket.Upgrader
}

type nodeStatus struct {
	Id          int
	Name        string
	State       string
	Term        int
	Commits     int
	CommitIndex int
}

type clusterStatus struct {
	Nodes []nodeStatus
}

func (h showcaseHandler) handlerMain(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "index.html")
}

func (h showcaseHandler) handlerStatus(w http.ResponseWriter, r *http.Request) {
	c, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Print("upgrade:", err)
		return
	}
	defer c.Close()

wsLoop:
	for {
		_, commandBytes, err := c.ReadMessage()
		if err != nil {
			log.Println("read:", err)
			break
		}
		if len(commandBytes) > 0 {
			command := nodeCommand{}
			if err := json.Unmarshal(commandBytes, &command); err != nil {
				log.Print(err.Error())
				continue
			}
			for _, node := range h.nodes {
				if node.id.String() == command.Node {
					resp, err := http.Post(fmt.Sprintf("http://%s/command", node.agentAddr), "", bytes.NewBuffer(commandBytes))
					if err != nil {
						log.Println(err.Error())
						continue wsLoop
					}
					body, err := io.ReadAll(resp.Body)
					if err != nil {
						log.Println(err.Error())
						continue wsLoop
					}
					resp.Body.Close()
					if resp.StatusCode != 200 {
						log.Printf("/command returned %d: `%s`", resp.StatusCode, body)
						continue wsLoop
					}
				}
			}

			if err := h.applyCommand(commandBytes); err != nil {
				log.Print(err.Error())
				continue
			}
		}
		cluster, err := h.fetchStatus()
		if err != nil {
			log.Print(err.Error())
			continue
		}

		clusterBytes, err := json.Marshal(&cluster)
		if err != nil {
			log.Println(err.Error())
			break
		}

		err = c.WriteMessage(websocket.TextMessage, clusterBytes)
		if err != nil {
			log.Println("write:", err)
			break
		}
	}
}

func (h showcaseHandler) applyCommand(command []byte) error {
	wg := sync.WaitGroup{}
	nodeErrCh := make(chan error, len(h.nodes))
	for _, node := range h.nodes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := http.Post(fmt.Sprintf("http://%s/apply", node.agentAddr), "", bytes.NewBuffer(command))
			if err != nil {
				nodeErrCh <- err
				return
			}
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				nodeErrCh <- err
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				nodeErrCh <- fmt.Errorf("%s", body)
				return
			}
		}()
	}
	wg.Wait()
	close(nodeErrCh)
	var errs error
	for err := range nodeErrCh {
		if err != nil {
			log.Print(err.Error())
			errs = errors.Join(errs, err)
		}
	}
	return errs
}

func (h showcaseHandler) fetchStatus() (clusterStatus, error) {
	wg := sync.WaitGroup{}
	nodeResponseCh := make(chan nodeStatus, len(h.nodes))
	nodeErrCh := make(chan error, len(h.nodes))
	for _, node := range h.nodes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := http.Get(fmt.Sprintf("http://%s/status", node.agentAddr))
			if err != nil {
				nodeErrCh <- err
				return
			}
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				nodeErrCh <- err
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				nodeErrCh <- fmt.Errorf("%s", body)
				return
			}
			nodeStatus := nodeStatus{}
			if err := json.Unmarshal(body, &nodeStatus); err != nil {
				nodeErrCh <- err
				return
			}
			nodeResponseCh <- nodeStatus
		}()
	}
	wg.Wait()
	close(nodeResponseCh)
	close(nodeErrCh)
	var errs error
	for err := range nodeErrCh {
		if err != nil {
			log.Print(err.Error())
			errs = errors.Join(errs, err)
		}
	}
	if errs != nil {
		return clusterStatus{}, errs
	}

	cluster := clusterStatus{}
	for status := range nodeResponseCh {
		cluster.Nodes = append(cluster.Nodes, status)
	}
	slices.SortFunc(cluster.Nodes, func(a nodeStatus, b nodeStatus) int {
		if a.Id > b.Id {
			return 1
		}
		return -1
	})
	return cluster, nil
}

func runDashboard(addr string, nodes []showcaseNode) {
	showcaseHandler := showcaseHandler{addr, nodes, websocket.Upgrader{}}
	http.HandleFunc("/", showcaseHandler.handlerMain)
	http.HandleFunc("/status", showcaseHandler.handlerStatus)
	log.Printf("run dashboard at %s", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatal(err.Error())
	}
}
