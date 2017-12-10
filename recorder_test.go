package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadEmpty(t *testing.T) {
	rf := filepath.Join(rundir, "test-topic")
	err := os.Remove(rf)
	if err != nil {
		t.Error("TestLoadEmpty failed when init")
	}
	r := NewRecorder(".", "test-topic", 10)
	if r == nil {
		t.Error("NewRecorder failed")
	}
	_, err = r.LoadOffset()
	if err == nil {
		t.Error("LoadOffset failed!", err)
	}
}

func TestWriteData(t *testing.T) {
	r := NewRecorder(".", "test-topic", 10)
	if r == nil {
		t.Error("NewRecorder failed")
	}

	for i := 0; i < 10; i++ {
		r.data[int32(i)] = int64(100 + i)
	}
	fmt.Println(r.data)
	r.RecordOffsets()
}

func TestLoad(t *testing.T) {
	r := NewRecorder(".", "test-topic", 10)
	if r == nil {
		t.Error("NewRecorder failed")
	}
	dataMap, err := r.LoadOffset()
	if err != nil {
		t.Error("LoadOffset failed!", err)
	}
	for k, v := range dataMap {
		fmt.Println(k, v)
	}
}

func TestLoadBackend(t *testing.T) {
	rf := filepath.Join(rundir, "test-topic")
	err := os.Remove(rf)
	if err != nil {
		t.Error("TestLoadBackend failed when init")
	}

	r := NewRecorder(".", "test-topic", 10)
	if r == nil {
		t.Error("NewRecorder failed")
	}

	r.RecordOffsetsBackend()

	func() {
		count := 0
		for {
			fmt.Println("update record")
			for i := 0; i < 10; i++ {
				r.Input <- &Record{Offset: int64(1000 + i*100 + count%10), Partition: int32(i)}
			}
			count++
			time.Sleep(3 * time.Second)
		}
	}()
}
