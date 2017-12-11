package main

import (
	"bytes"
	"time"

	"github.com/Shopify/sarama"
	log "github.com/chenhuaying/glog"
)

const (
	RecordThreshold       = 1000
	TimeThreshold   int64 = 1
)

var Topic2TypeTable map[string]string = map[string]string{
	"4g_info":  "4G",
	"23g_info": "23G",
}

func harvestorType(topic string) string {
	if Topic2TypeTable[topic] == "" {
		return "Unknown"
	} else {
		return Topic2TypeTable[topic]
	}
}

type harvestor struct {
	handler   sarama.PartitionConsumer
	partition int
}

func newHarvestor(partition int, h sarama.PartitionConsumer) *harvestor {
	return &harvestor{handler: h, partition: partition}
}

func (h *harvestor) harvest(flag string, output chan []byte, record chan *Record) {
	var buf bytes.Buffer
	count := 0
	lasttime := time.Now().Unix()
	threshold := RecordThreshold
	timeThreshold := TimeThreshold
	for {
		select {
		case msg := <-h.handler.Messages():
			log.Debugf("Consumed message topic %s, Partition %d, offset %d, key(%s), msg(%s)\n", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
			fields, err := ParseSignaling(msg.Value)
			if err != nil {
				log.Warning(err)
			} else {
				for _, f := range fields {
					buf.Write(f)
					buf.WriteString("|")
				}
				buf.WriteString(flag)
				buf.WriteString("\n")
				log.Debug(buf.String())
				tmp := buf.Bytes()
				item := make([]byte, len(tmp))
				copy(item, tmp)
				output <- item
				buf.Reset()

				count++
				now := time.Now().Unix()
				if count%threshold == 0 || now-lasttime >= timeThreshold {
					record <- &Record{Offset: msg.Offset, Partition: msg.Partition}
					if count%threshold == 0 {
						log.Infof("Consumed message topic %s, Partition %d, offset %d, Processed(%d)\n", msg.Topic, msg.Partition, msg.Offset, count)
						count = 0
					}
				}
			}
		case err := <-h.handler.Errors():
			if err != nil {
				log.Errorf("Topic %s, Partition: %d, error: %s\n", err.Topic, err.Partition, err.Err)
			} else {
				log.Error("Unkonw Error")
			}
		}
	}
}

type HarvestorGroup struct {
	topic      string
	addrs      []string
	harvestors []*harvestor
	client     sarama.Consumer
	partitions int
	flag       string
	recorder   *Recorder
}

func NewHarvestorGroup(addrs []string, topic string, partitions int) *HarvestorGroup {
	g := HarvestorGroup{
		topic:      topic,
		addrs:      addrs,
		partitions: partitions,
		flag:       harvestorType(topic),
		recorder:   NewRecorder(rundir, topic, partitions),
	}

	client, err := sarama.NewConsumer(addrs, nil)
	if err != nil {
		log.Errorf("Open kafka consumer failed! error: %s", err)
		return nil
	}
	g.client = client

	return &g
}

func (g *HarvestorGroup) Init() error {
	useNewest := true
	offsets, err := g.recorder.LoadOffset()
	if err != nil {
		log.Warningf("LoadOffset failed, use the Newest! error: %s", err)
	} else {
		useNewest = false
	}

	for i := 0; i < g.partitions; i++ {
		// test use sarama.OffsetOldest is ok, others use OffsetNewest temporarily
		offset := sarama.OffsetNewest
		if !useNewest {
			offset = offsets[int32(i)]
		}
		c, err := g.client.ConsumePartition(g.topic, int32(i), offset)
		if err != nil {
			return err
		}

		h := newHarvestor(i, c)
		g.harvestors = append(g.harvestors, h)
	}

	for i := 0; i < len(g.harvestors); i++ {
		log.Debug(g.harvestors[i].partition)
	}

	g.recorder.RecordOffsetsBackend()

	return nil
}

func (g *HarvestorGroup) Harvest(output chan []byte) {
	for i, h := range g.harvestors {
		log.Infof("Topic %s, havestor %d, harvest\n", g.topic, i)
		go h.harvest(g.flag, output, g.recorder.Input)
	}
}

func (g *HarvestorGroup) Close() {
	for i := 0; i < len(g.harvestors); i++ {
		if err := g.harvestors[i].handler.Close(); err != nil {
			log.Fatalln(err)
		}
	}
	if err := g.client.Close(); err != nil {
		log.Fatalln(err)
	}
}
