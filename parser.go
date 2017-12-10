package main

import (
	"bytes"
	"errors"
)

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
