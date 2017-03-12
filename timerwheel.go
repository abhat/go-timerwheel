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

// interface class to provide timer attributes
type Timer interface {
	Periodic() bool                //is this timer periodic
	Interval() int64               //what is the interval of the timer in nano-seconds
	Oneshot() bool                 //is this timer oneshot
	Expired()                      //routine to call when the timer expires
	Name() string                  //name of the timer (key for the timer wheel)
	NextExpiration() int64         //when will the timer expire next
	SetNextExpiration(int64) error //set when the timer will expire next
	StartTime() int64              //set when the timer should fire first in nanoseconds
}

// timer wheel type definition
type Timerwheel struct {
	precision     time.Duration // precision time duration in microseconds specified as (N * time.Microsecond)
	maxTimers     int           // number of max timers to be supported
	timers        map[string]Timer
	lock          sync.Mutex
	ticker        *time.Ticker
	suspended     bool
	deletedTimers []Timer
}

// create a new timer wheel
func NewTimerWheel(precision time.Duration, maxTimers int) *Timerwheel {
	return &Timerwheel{
		precision:     precision,
		maxTimers:     maxTimers,
		timers:        make(map[string]Timer),
		ticker:        time.NewTicker(precision),
		suspended:     true,
		deletedTimers: make([]Timer, 0),
	}
}

// add a timer to the timerwheel
func (tw *Timerwheel) Addtimer(t Timer) error {
	tw.lock.Lock()
	defer tw.lock.Unlock()
	if len(tw.timers) < tw.maxTimers {
		t.SetNextExpiration(time.Now().UnixNano() + t.StartTime())
		tw.timers[t.Name()] = t
		if len(tw.timers) == 1 && tw.suspended {
			tw.suspended = false
			go tw.Run()
		}

		return nil
	}
	return errors.New("Already have max timers in the timer wheel")
}

// delete a timer from timer wheel by name
func (tw *Timerwheel) Deletetimer(t Timer) error {
	tw.lock.Lock()
	defer tw.lock.Unlock()
	return tw.deleteTimer(t)
}

// internal unprotected method to delete timer
func (tw *Timerwheel) deleteTimer(t Timer) error {
	delete(tw.timers, t.Name())
	if len(tw.timers) == 0 {
		tw.suspended = true
	}
	return nil
}

// run method for a timer wheel

func (tw *Timerwheel) Run() {
	for _ = range tw.ticker.C {
		if tw.suspended {
			return
		}
		tw.lock.Lock()
		for _, timer := range tw.timers {
			if timer.NextExpiration() <= time.Now().UnixNano() {
				timer.Expired()
				if timer.Periodic() {
					timer.SetNextExpiration(time.Now().UnixNano() + timer.Interval())
				} else {
					tw.deletedTimers = append(tw.deletedTimers, timer)
				}
			}
		}
		for _, timer := range tw.deletedTimers {
			tw.deleteTimer(timer)
		}
		tw.deletedTimers = tw.deletedTimers[:0]
		tw.lock.Unlock()
	}
}

// is the timer wheel running?
func (tw *Timerwheel) Running() bool {
	return !tw.suspended
}
