package main

import (
	"fmt"
	"os"
	"os/signal"

	log "github.com/chenhuaying/glog"
	"github.com/spf13/viper"
)

var (
	logLevel      = "INFO"
	consoleLevel  = "ERROR"
	flushInterval = "10s"

	workdir = "."
	rundir  = workdir + "/run"
)

func main() {
	viper.SetConfigName("config")
	// priority: . > $HOME/.signaling-harvestor > /etc/signaling-harvestor
	viper.AddConfigPath(".")
	viper.AddConfigPath("$HOME/.signaling-harvestor")
	viper.AddConfigPath("/etc/signaling-harvestor")

	if err := viper.ReadInConfig(); err != nil {
		log.Error("read config error: ", err)
	}

	logLevel = viper.GetString("logLevel")
	consoleLevel = viper.GetString("consoleLevel")
	flushInterval = viper.GetString("flushInterval")

	topics := viper.GetStringMap("topics")
	for topic, partitionNum := range topics {
		fmt.Println(topic, partitionNum)
	}

	brokerList := viper.GetStringSlice("broker_list")
	fmt.Println(brokerList)

	fmt.Println(logLevel, consoleLevel, flushInterval)
	log.Init(log.Options{LogLevel: logLevel, ConsoleLevel: consoleLevel, FlushInterval: flushInterval})

	publisher := NewPublisher()
	go publisher.Publish()

	//harvestorGroup := NewHarvestorGroup(brokerList, "4g_info", topics["4g_info"].(int))
	//harvestorGroup.Init()
	//defer harvestorGroup.Close()
	harvestorGroups := []*HarvestorGroup{}
	for topic, partitionNum := range topics {
		harvestorGroup := NewHarvestorGroup(brokerList, topic, partitionNum.(int))
		if harvestorGroup == nil {
			panic("Create HarvestorGroup failed!")
		}
		err := harvestorGroup.Init()
		if err != nil {
			log.Errorf("Init %s harvestor group failed! error: %s\n", topic, err)
		}
		harvestorGroup.Harvest(publisher.msgCh)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

MainLoop:
	for {
		select {
		case <-signals:
			break MainLoop
		}
	}

	for _, g := range harvestorGroups {
		g.Close()
	}
}
