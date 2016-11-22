package sgload

import "time"

type MockStatter struct{}

func (ms MockStatter) Counter(sampleRate float32, bucket string, n ...int) {}

func (ms MockStatter) Timing(sampleRate float32, bucket string, d ...time.Duration) {}

func (ms MockStatter) Gauge(sampleRate float32, bucket string, value ...string) {}
