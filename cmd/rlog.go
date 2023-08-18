package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/textproto"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

type entry struct {
	Group   string    `json:"group"`
	Time    time.Time `json:"time"`
	Level   string    `json:"level"`
	Message string    `json:"message"`
}

func main() {
	host := getenv("RLOG_HOST", "")
	port := getenv("RLOG_PORT", "9898")
	protocol := "tcp"
	address := fmt.Sprintf("%s:%s", host, port)
	server, err := net.Listen(protocol, address)
	log.Println("application started, listening on " + address)

	if err != nil {
		log.Fatalf("error starting server. %v\n", err)
	}

	defer server.Close()
	data := make(chan []byte, 100)
	go useData(data)

	for {
		conn, err := server.Accept()
		if err != nil {
			log.Printf("error accepting connection. %v\n", err)
		}
		go getData(conn, data)
	}
}

func getData(conn net.Conn, data chan []byte) {
	defer conn.Close()
	idlCfg := getenv("RLOG_IDLE_TIMEOUT_MIN", "15")
	idleTimeout := 15

	if i, err := strconv.Atoi(idlCfg); err != nil {
		idleTimeout = i
	}

	buf := bufio.NewReader(conn)
	tp := textproto.NewReader(buf)

	for {
		line, err := tp.ReadLineBytes()
		if err != nil && (errors.Is(err, io.EOF) || errors.Is(err, os.ErrDeadlineExceeded)) {
			fmt.Printf("stop reading. %v\n", err)
			break
		}
		conn.SetReadDeadline(time.Now().Add(time.Duration(idleTimeout) * time.Minute))
		data <- line
	}
}

func useData(data chan []byte) {
	entryGroup := make(map[string][]byte)
	interval := 10
	intvlCfg := getenv("RLOG_INTERVAL_SEC", "10")

	if i, err := strconv.Atoi(intvlCfg); err != nil {
		interval = i
	}

	ticker := time.NewTicker(time.Duration(interval) * time.Second)

	for {
		select {
		case <-ticker.C:
			persistEntries(entryGroup)
			ticker.Reset(time.Duration(interval) * time.Second)
		case d := <-data:
			var e entry

			if err := json.Unmarshal(d, &e); err != nil {
				// log error
				continue
			}

			d = append(d, byte('\n'))
			line := entryGroup[e.Group]
			line = append(line, d...)
			entryGroup[e.Group] = line
		}
	}
}

func persistEntries(entryGroup map[string][]byte) {
	for group, entries := range entryGroup {
		log.Printf("persisiting %d bytes on group %s", len(entries), group)
		file, err := openLogFile(group)

		if err != nil {
			log.Printf("[ERROR] failed opening log file. %v\n", err)
			continue
		}

		defer file.Close()
		writer := bufio.NewWriter(file)
		writer.Write(entries)
		writer.Flush()
		delete(entryGroup, group)
	}
}

func openLogFile(group string) (*os.File, error) {
	date := time.Now()
	year := fmt.Sprint(date.Year())
	month := fmt.Sprint(int(date.Month()))
	day := fmt.Sprint(date.Day())
	filename := fmt.Sprintf("%s_%s-%s-%s.log", group, year, month, day)
	baseDir := "."
	logDir := filepath.Join(baseDir, year, month, day)
	logFile := filepath.Join(logDir, filename)
	file, err := os.OpenFile(logFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, os.ModePerm)

	if err == nil {
		return file, nil
	}

	stat, err := os.Stat(logDir)

	if (err != nil && os.IsNotExist(err)) || !stat.IsDir() {
		if err = os.MkdirAll(logDir, os.ModePerm); err != nil {
			return nil, err
		}
	}

	file, err = os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, os.ModePerm)

	if err != nil {
		return nil, err
	}

	return file, nil
}

func getenv(key string, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
