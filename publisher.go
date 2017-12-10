package main

import (
	"io"

	log "github.com/chenhuaying/glog"
)

const PUBLISHERSIZE = 10240

var (
	basePath = "./data/data"
)

type Publisher struct {
	msgCh chan []byte
	out   io.WriteCloser
}

func NewPublisher() *Publisher {
	out, err := NewRollingFile(basePath, MinutelyRolling)
	if err != nil {
		log.Fatal(err)
	}
	return &Publisher{msgCh: make(chan []byte, PUBLISHERSIZE), out: out}
}

func (p *Publisher) Publish() {
	for {
		select {
		case msg := <-p.msgCh:
			log.Debug("Publish: ", msg)
			p.out.Write(msg)
		}
	}
}
