package main

import (
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

var (
	clients   = make(map[*websocket.Conn]bool)
	clientsMu sync.Mutex
)

func broadcast(sender *websocket.Conn, msg []byte) {
	clientsMu.Lock()
	defer clientsMu.Unlock()
	for conn := range clients {
		if conn == sender {
			continue
		}
		if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
			log.Println("write error:", err)
			conn.Close()
			delete(clients, conn)
		}
	}
}

func handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("upgrade error:", err)
		return
	}
	defer func() {
		clientsMu.Lock()
		delete(clients, conn)
		clientsMu.Unlock()
		conn.Close()
	}()

	clientsMu.Lock()
	clients[conn] = true
	clientsMu.Unlock()

	fmt.Println("new client connected, total:", len(clients))

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			log.Println("client disconnected:", err)
			break
		}
		fmt.Printf("received: %s\n", msg)
		broadcast(conn, msg)
	}
}

func main() {
	http.HandleFunc("/ws", handleWS)
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "index.html")
	})
	log.Println("server running on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
