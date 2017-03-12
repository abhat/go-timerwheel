/*
###########################################################################
#
#   Filename:           timerwheel.go
#
#   Author:             Aniket Bhat
#   Created:            03/10/2017
#
#   Description:        Implements a timer wheel to do a scalable multi-timer
#                       implementation
#
###########################################################################
#
#              Copyright (c) 2017 Aniket Bhat
#
###########################################################################
*/

package timerwheel

import (
	"errors"
	"sync"
	"time"
)

type Timer interface {
	Periodic() bool                //is this timer periodic
	Interval() int64               //what is the interval of the timer in nano-seconds
	Oneshot() bool                 //is this timer oneshot
	Expired()                      //routine to call when the timer expires
	Name() string                  //name of the timer (key for the timer wheel)
	GetNextExpiration() int64      //when will the timer expire next
	SetNextExpiration(int64) error //set when the timer will expire next
	GetStartTime() int64           //set when the timer should fire first in nanoseconds
}

type Timerwheel struct {
	precision time.Duration // precision time duration in microseconds specified as (N * time.Microsecond)
	maxTimers int           // number of max timers to be supported
	timers    map[string]Timer
	lock      sync.Mutex
	ticker    *time.Ticker
	suspended bool
}

func NewTimerWheel(precision time.Duration, maxTimers int) *Timerwheel {
	return &Timerwheel{
		precision: precision,
		maxTimers: maxTimers,
		timers:    make(map[string]Timer),
		ticker:    time.NewTicker(precision),
		suspended: true,
	}
}

func (tw *Timerwheel) Addtimer(t Timer) error {
	tw.lock.Lock()
	defer tw.lock.Unlock()
	if len(tw.timers) < tw.maxTimers {
		tw.timers[t.Name()] = t
		t.SetNextExpiration(time.Now().UnixNano() + t.GetStartTime())
		if len(tw.timers) == 1 {
			tw.suspended = false
			go tw.Run()
		}
		return nil
	}
	return errors.New("Already have max timers in the timer wheel")
}

func (tw *Timerwheel) Deletetimer(t Timer) error {
	tw.lock.Lock()
	defer tw.lock.Unlock()
	return tw.deleteTimer(t)
}

func (tw *Timerwheel) deleteTimer(t Timer) error {
	delete(tw.timers, t.Name())
	if len(tw.timers) == 0 {
		tw.suspended = true
	}
	return nil
}

func (tw *Timerwheel) Run() {
	for _ = range tw.ticker.C {
		tw.lock.Lock()
		defer tw.lock.Unlock()
		if tw.suspended {
			break
		}
		for _, timer := range tw.timers {
			if timer.GetNextExpiration() <= time.Now().UnixNano() {
				if timer.Periodic() {
					timer.SetNextExpiration(time.Now().UnixNano() + timer.Interval())
				} else {
					tw.deleteTimer(timer)
				}
				timer.Expired()
			}
		}
	}
}

func (tw *Timerwheel) Running() bool {
	return tw.suspended
}
