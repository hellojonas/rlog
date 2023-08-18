package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

func _TestReader(t *testing.T) {
	buf := bytes.NewBuffer(make([]byte, 0))
	b := []byte("line1\nline2\nline3")
	if _, err := buf.Write(b); err != nil {
		t.Fatalf("error writing log entry. %v\n", err)
	}
	reader := bufio.NewReader(buf)
	for reader.Buffered() > 0 {
		line, _, err := reader.ReadLine()
		if err != nil {
			break
		}
		fmt.Printf("---> %s\n", string(line))
	}
	buf.Reset()
	b = []byte("line4\nline5\nline6")
	if _, err := buf.Write(b); err != nil {
		t.Fatalf("error writing log entry. %v\n", err)
	}
	for reader.Buffered() > 0 {
		line, _, err := reader.ReadLine()
		if err != nil {
			break
		}
		fmt.Printf("---> %s\n", string(line))
	}
}

func TestSendLogs(t *testing.T) {
	c1, err := net.Dial("tcp", "localhost:9898")

	if err != nil {
		t.Fatalf("error dialing server. %v\n", err)
	}

	defer c1.Close()
	groups := 10
	lines := 10000
	wg := sync.WaitGroup{}

	for i := 0; i < groups; i++ {
		wg.Add(1)
		go func(group int) {
			for j := 0; j < lines/2; j++ {
				e := entry{
					Group:   fmt.Sprintf("group%d", group+1),
					Time:    time.Now(),
					Level:   "INFO",
					Message: fmt.Sprintf("Log entry number %4d", j),
				}
				bEntry, _ := json.Marshal(e)
				bEntry = append(bEntry, byte('\n'))
				c1.Write(bEntry)
				time.Sleep(time.Duration(20) * time.Millisecond)
			}
			wg.Done()
		}(i)
	}

	c2, err := net.Dial("tcp", "localhost:9898")

	if err != nil {
		t.Fatalf("error dialing server. %v\n", err)
	}

	defer c2.Close()

	for i := 0; i < groups; i++ {
		wg.Add(1)
		go func(group int) {
			for j := lines / 2; j < lines; j++ {
				e := entry{
					Group:   fmt.Sprintf("group%d", group+1),
					Time:    time.Now(),
					Level:   "INFO",
					Message: fmt.Sprintf("Log entry number %4d", j),
				}
				bEntry, _ := json.Marshal(e)
				bEntry = append(bEntry, byte('\n'))
				c2.Write(bEntry)
				time.Sleep(time.Duration(20) * time.Millisecond)
			}
			wg.Done()
		}(i)
	}

	wg.Wait()

	select {
	case <-time.After(5 * time.Minute):
		fmt.Println("done!")
	}
}
