package main

import (
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"time"

	// "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Header struct {
	Size int32
	APIKey int16
	APIVersion int16 
}

type Message struct {
	data []byte
}

type Server struct {
	coffsets map[string]int
	buffer   []Message

	ln net.Listener
}

func NewServer() *Server {
	return &Server{
		coffsets: make(map[string]int),
		buffer:   make([]Message, 0),
	}
}

func (s *Server) Listen() error {
	ln, err := net.Listen("tcp", "8000")
	if err != nil {
		return err
	}
	s.ln = ln
	for {
		conn, err := ln.Accept()
		if err != nil {
			if err == io.EOF {
				return err
			}
			slog.Error("server accept error", " err", err)
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {

	buf := make([]byte, 1024)

	for {
		n, err := conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				return
			}
			slog.Error("connection read error", "err", err)
			return
		}
		rawMsg := buf[:n]
		fmt.Println(rawMsg)
	}
}

func main() {
	server := NewServer()
	go func() {
		log.Fatal(server.Listen())
	}()
	time.Sleep(time.Second)
}
