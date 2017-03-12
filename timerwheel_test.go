/*
###########################################################################
#
#   Filename:           timerwheel_test.go
#
#   Author:             Aniket Bhat
#   Created:            03/10/2017
#
#   Description:        Test code for timer wheel
#
###########################################################################
#
#              Copyright (c) 2017 Aniket Bhat
#
###########################################################################
*/

package timerwheel

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)

type MockTimer struct {
	periodic       bool
	interval       int64
	startTime      int64
	contextData    string
	index          int
	nextExpiration int64
}

func NewMockTimer(periodic bool, startTime, interval int64, index int, contextData string) *MockTimer {
	return &MockTimer{
		periodic:    periodic,
		startTime:   startTime,
		interval:    interval,
		contextData: contextData,
		index:       index,
	}
}

func (mt *MockTimer) Periodic() bool {
	return mt.periodic
}

func (mt *MockTimer) Oneshot() bool {
	return !mt.periodic
}

func (mt *MockTimer) Interval() int64 {
	return mt.interval
}

func (mt *MockTimer) Name() string {
	return "MockTimer-" + strconv.Itoa(mt.index)
}

func (mt *MockTimer) NextExpiration() int64 {
	return mt.nextExpiration
}

func (mt *MockTimer) SetNextExpiration(exp int64) error {
	mt.nextExpiration = exp
	return nil
}

func (mt *MockTimer) StartTime() int64 {
	return mt.startTime
}

func (mt *MockTimer) Expired() {
	fmt.Printf("Timer %v expired - context data is %v\n", mt.Name(), mt.contextData)
}

func TestTimerWheelMaxTimers(t *testing.T) {
	tw := NewTimerWheel(5*time.Millisecond, 3)
	if tw != nil {
		t1 := NewMockTimer(false, int64(1*time.Second), int64(10*time.Millisecond), 1, "I am number one")
		err := tw.Addtimer(t1)
		t2 := NewMockTimer(true, int64(2*time.Second), int64(20*time.Millisecond), 2, "I am number two")
		err = tw.Addtimer(t2)
		t3 := NewMockTimer(true, int64(3*time.Second), int64(30*time.Millisecond), 3, "I am number three")
		err = tw.Addtimer(t3)
		err = tw.Addtimer(NewMockTimer(false, int64(4*time.Second), int64(25*time.Millisecond), 4, "I am number four"))
		if err == nil {
			t.Fail()
		}
		runDuration := time.NewTicker(10 * time.Second)
		select {
		case <-runDuration.C:
			tw.Deletetimer(t2)
			tw.Deletetimer(t3)
		}
		if tw.Running() {
			t.Fail()
		}
	}
}

func TestTimerWheel(t *testing.T) {
	tw := NewTimerWheel(5*time.Millisecond, 3)
	if tw != nil {
		go tw.Run()
		t1 := NewMockTimer(false, int64(1*time.Second), int64(10*time.Millisecond), 1, "I am number one")
		err := tw.Addtimer(t1)
		t2 := NewMockTimer(true, int64(2*time.Second), int64(20*time.Millisecond), 2, "I am number two")
		err = tw.Addtimer(t2)
		t3 := NewMockTimer(true, int64(3*time.Second), int64(30*time.Millisecond), 3, "I am number three")
		err = tw.Addtimer(t3)
		if err != nil {
			t.Fail()
		}
		runDuration := time.NewTicker(10 * time.Second)
		select {
		case <-runDuration.C:
			tw.Deletetimer(t2)
			tw.Deletetimer(t3)
		}
	}

}
