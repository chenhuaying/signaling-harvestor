package main

import (
	"brdserver/cache"
	"time"
)

var ccFilter = cache.New()
var ccClearTime time.Time = time.Now().Add(-240 * time.Hour)

type sdata struct {
	occ  time.Time
	laci string
}

// true: message may write to file
// false: skip this message
// params:
//   uid:user id
//   occstr: event_time
//   laci: lac_ci
func Filter(occstr, uid, laci string) bool {
	occ, _ := time.ParseInLocation("2006-01-02 15:04:05", occstr, time.Local)

	if occ.Sub(ccClearTime).Hours() > 24 {
		ccClearTime = occ
		for k, v := range ccFilter.Items() {
			if ccClearTime.After(v.(time.Time)) {
				ccFilter.Remove(k)
			}
		}
	}

	ret, ok := ccFilter.Get(uid)
	if !ok {
		data := &sdata{
			occ:  occ,
			laci: laci,
		}
		ccFilter.Set(uid, data)
		return true
	}

	if occ.Sub(ret.(*sdata).occ).Seconds() < 59 && laci == ret.(*sdata).laci {
		return false
	}
	data := &sdata{
		occ:  occ,
		laci: laci,
	}
	ccFilter.Set(uid, data)
	return true
}
