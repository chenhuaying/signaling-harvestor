package main

import (
	"bytes"
	"errors"

	log "github.com/chenhuaying/glog"
)

func ParseSignalings(msgs []byte, flag string, output chan []byte) (int, int) {
	msg_list := bytes.Split(msgs, []byte("\n"))

	count := 0
	errCount := 0
	var buf bytes.Buffer
	for _, msg := range msg_list {
		if len(msg) == 0 {
			continue
		}
		fields, err := ParseSignaling(msg)
		if err != nil {
			log.Debugf("ParseSignaling Failed! msg(%s) error: %s\n", msg, err)
			errCount++
			continue
		} else {
			if !Filter(string(fields[0]), string(fields[1]), string(fields[3])) {
				continue
			}
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
		}
	}

	return count, errCount
}

func ParseSignaling(msg []byte) ([][]byte, error) {
	fields := bytes.Split(msg, []byte("|"))
	// 0 => isdn, 6 => lac, 7 => ci, 17 => event_time, 18 => phone_prefix
	if string(fields[0]) == "" || string(fields[6]) == "" || string(fields[7]) == "" || string(fields[17]) == "" || string(fields[18]) == "" {
		return nil, errors.New("signaling format error")
	}
	lac := fields[6]
	ci := fields[7]
	size := len(lac) + len(ci) + 1
	lacci := make([]byte, size, size)
	copy(lacci, lac)
	lacci[len(lac)] = '_'
	copy(lacci[len(lac)+1:size], ci)

	return [][]byte{fields[17], fields[0], fields[18], lacci}, nil
}
