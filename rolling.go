// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
	//"github.com/CodisLabs/codis/pkg/utils/errors"
	"errors"
)

type rollingFile struct {
	mu sync.Mutex

	closed bool

	file     *os.File
	basePath string
	filePath string
	fileFrag string

	rolling RollingFormat
}

var ErrClosedRollingFile = errors.New("rolling file is closed")

type RollingFormat string

const (
	MonthlyRolling  RollingFormat = "200601"
	DailyRolling                  = "20060102"
	HourlyRolling                 = "2006010215"
	MinutelyRolling               = "200601021504"
	SecondlyRolling               = "20060102150405"
)

func (r *rollingFile) roll() error {
	suffix := time.Now().Format(string(r.rolling))
	if r.file != nil {
		if suffix == r.fileFrag {
			return nil
		}
		r.file.Close()
		r.file = nil
	}
	r.fileFrag = suffix

	dir, _ := filepath.Split(r.basePath)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0777); err != nil {
			return err
		}
	}

	filename := fmt.Sprintf("%s.txt", r.fileFrag)
	r.filePath = filepath.Join(dir, filename)

	f, err := os.OpenFile(r.filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return err
	} else {
		r.file = f
		return nil
	}
}

func (r *rollingFile) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil
	}

	r.closed = true
	if f := r.file; f != nil {
		r.file = nil
		if err := f.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (r *rollingFile) Write(b []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return 0, ErrClosedRollingFile
	}

	if err := r.roll(); err != nil {
		return 0, err
	}

	n, err := r.file.Write(b)
	if err != nil {
		return n, err
	} else {
		return n, nil
	}
}

func NewRollingFile(basePath string, rolling RollingFormat) (io.WriteCloser, error) {
	if _, file := filepath.Split(basePath); file == "" {
		errMsg := fmt.Sprintf("invalid base-path = %s, file name is required", basePath)
		return nil, errors.New(errMsg)
	}
	return &rollingFile{basePath: basePath, rolling: rolling}, nil
}
