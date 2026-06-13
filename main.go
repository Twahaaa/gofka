package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"time"

	// "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type APIVersion struct {
	CorrelationID         int32
	ClientSoftwareName    []byte
	ClientSoftwareVersion []byte
}

func readAPIVersion(r io.ByteReader) APIVersion {
	var version APIVersion
	binary.Read(r.(io.Reader), binary.BigEndian, &version.CorrelationID)
	size, _ := binary.ReadUvarint(r)
	fmt.Println("the version is", version)
	fmt.Println("the size is", size)
	return APIVersion{}
}

type Header struct {
	Size       int32
	APIKey     int16
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
	ln, err := net.Listen("tcp", ":9092")
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

		r := bytes.NewReader(rawMsg)
		var header Header
		binary.Read(r, binary.BigEndian, &header)
		fmt.Println(header)

		readAPIVersion(r)
	}
}

func main() {
	server := NewServer()
	go func() {
		log.Fatal(server.Listen())
	}()
	time.Sleep(time.Second)

	fmt.Println("producing...")

	err := StartProducer("prod", "hello from kafka prod")
	if err != nil {
		slog.Error("error from producer", "err", err)
	}

	run := true
	topics := []string{"prod"}
	err = StartConsumer(topics, &run)

	if err != nil {
		slog.Error("error from consumer", "err", err)
	}

	
}
