package main

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"time"

	log "github.com/chenhuaying/glog"
)

type Record struct {
	Offset    int64
	Partition int32
}

type Offsets map[int32]int64

type Recorder struct {
	Input  chan *Record
	data   Offsets
	source *os.File
}

func NewRecorder(workdir, topic string, chSize int) *Recorder {
	recordPath := path.Join(rundir, topic)

	if dir, _ := filepath.Split(recordPath); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			log.Error("Recorder mkdir failed! error: %s", err)
			return nil
		}
	}

	f, err := os.OpenFile(recordPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Error("Open Record file %s failed! error: %s", recordPath, err)
		return nil
	}
	return &Recorder{Input: make(chan *Record, chSize), data: make(Offsets), source: f}
}

func (r *Recorder) LoadOffset() (Offsets, error) {
	data, err := ioutil.ReadAll(r.source)
	if err != nil {
		return nil, err
	}

	var dataMap Offsets
	err = json.Unmarshal(data, &dataMap)
	if err != nil {
		return nil, err
	}
	return dataMap, nil
}

func (r *Recorder) RecordOffsets() {
	dataBytes, err := json.Marshal(r.data)
	if err != nil {
		log.Error("Encoding data failed")
	} else {
		r.source.Write(dataBytes)
		r.source.Seek(0, os.SEEK_SET)
	}
}

func (r *Recorder) RecordOffsetsBackend() {
	flush := time.Tick(3 * time.Second)
	go func() {
		for {
			select {
			case record := <-r.Input:
				r.data[record.Partition] = record.Offset
			case <-flush:
				dataBytes, err := json.Marshal(r.data)
				if err != nil {
					log.Error("Encoding data failed")
				} else {
					r.source.Write(dataBytes)
					r.source.Seek(0, os.SEEK_SET)
				}
			}
		}
	}()
}
