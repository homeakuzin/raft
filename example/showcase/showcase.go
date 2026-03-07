package main

import (
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

type showcaseHandler struct {
	addr     string
	nodes    []showcaseNode
	upgrader websocket.Upgrader
}

type nodeStatus struct {
	Id     int
	State  string
	Term   int
	Commit int
}

var homeTemplate = template.Must(template.New("").Parse(`
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<script>  
window.addEventListener("load", function(evt) {

    var output = document.getElementById("output");
    var input = document.getElementById("input");
    var ws;

    var print = function(message) {
        var d = document.createElement("div");
        d.textContent = message;
        output.appendChild(d);
        output.scroll(0, output.scrollHeight);
    };

    document.getElementById("open").onclick = function(evt) {
        if (ws) {
            return false;
        }
        ws = new WebSocket("{{.}}");
        ws.onopen = function(evt) {
            print("OPEN");
        }
        ws.onclose = function(evt) {
            print("CLOSE");
            ws = null;
        }
        ws.onmessage = function(evt) {
            print("RESPONSE: " + evt.data);
        }
        ws.onerror = function(evt) {
            print("ERROR: " + evt.data);
        }
        return false;
    };

    document.getElementById("send").onclick = function(evt) {
        if (!ws) {
            return false;
        }
        print("SEND: " + input.value);
        ws.send(input.value);
        return false;
    };

    document.getElementById("close").onclick = function(evt) {
        if (!ws) {
            return false;
        }
        ws.close();
        return false;
    };

});
</script>
</head>
<body>
<table>
<tr><td valign="top" width="50%">
<p>Click "Open" to create a connection to the server, 
"Send" to send a message to the server and "Close" to close the connection. 
You can change the message and send multiple times.
<p>
<form>
<button id="open">Open</button>
<button id="close">Close</button>
<p><input id="input" type="text" value="Hello world!">
<button id="send">Send</button>
</form>
</td><td valign="top" width="50%">
<div id="output" style="max-height: 70vh;overflow-y: scroll;"></div>
</td></tr></table>
</body>
</html>
`))

type clusterStatus struct {
	Nodes []nodeStatus
}

func (h showcaseHandler) handlerMain(w http.ResponseWriter, r *http.Request) {
	homeTemplate.Execute(w, "ws://"+r.Host+"/status")

}

func (h showcaseHandler) handlerStatus(w http.ResponseWriter, r *http.Request) {
	c, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Print("upgrade:", err)
		return
	}
	defer c.Close()

	for {
		_, message, err := c.ReadMessage()
		if err != nil {
			log.Println("read:", err)
			break
		}
		log.Printf("recv: %s", message)

		wg := sync.WaitGroup{}
		statusesCh := make(chan nodeStatus, len(h.nodes))
		statusErrCh := make(chan error, len(h.nodes))
		for _, node := range h.nodes {
			wg.Add(1)
			go func() {
				defer wg.Done()
				resp, err := http.Get(fmt.Sprintf("http://%s/status", node.agentAddr))
				if err != nil {
					statusErrCh <- err
					return
				}
				body, err := io.ReadAll(resp.Body)
				if err != nil {
					statusErrCh <- err
					return
				}
				defer resp.Body.Close()
				if resp.StatusCode != 200 {
					statusErrCh <- fmt.Errorf("%s", body)
					return
				}
				nodeStatus := nodeStatus{}
				if err := json.Unmarshal(body, &nodeStatus); err != nil {
					statusErrCh <- err
					return
				}
				statusesCh <- nodeStatus
			}()
		}
		wg.Wait()
		close(statusesCh)
		close(statusErrCh)
		var statusErr error
		for err := range statusErrCh {
			if err != nil {
				statusErr = err
				break
			}
		}
		if statusErr != nil {
			log.Printf("/status: %s", statusErr.Error())
			err = c.WriteMessage(websocket.TextMessage, []byte(statusErr.Error()))
			continue
		}

		cluster := clusterStatus{}
		for status := range statusesCh {
			cluster.Nodes = append(cluster.Nodes, status)
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

func runDashboard(addr string, nodes []showcaseNode) {
	showcaseHandler := showcaseHandler{addr, nodes, websocket.Upgrader{}}
	http.HandleFunc("/", showcaseHandler.handlerMain)
	http.HandleFunc("/status", showcaseHandler.handlerStatus)
	log.Printf("run dashboard at %s", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatal(err.Error())
	}
}
