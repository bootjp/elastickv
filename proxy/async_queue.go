package proxy

import (
	"context"
	"time"
)

const (
	asyncQueueWrite    = "write"
	asyncQueueScript   = "script"
	asyncQueueBlocking = "blocking"
	asyncQueueShadow   = "shadow"

	asyncDropQueueFull = "queue_full"
	asyncDropExpired   = "expired"
)

type asyncTask struct {
	enqueuedAt time.Time
	deadline   time.Time
	fn         func(context.Context)
}

func (d *DualWriter) startAsyncDispatchers() {
	d.dispatchWG.Add(1)
	go d.dispatchAsyncQueue(d.writeQueue, d.writeQueueSlots, asyncQueueWrite, d.writeSem)
	d.dispatchWG.Add(1)
	go d.dispatchAsyncQueue(d.scriptQueue, d.scriptQueueSlots, asyncQueueScript, d.scriptSem, d.writeSem)
	if cap(d.blockingReplaySem) > 0 {
		d.dispatchWG.Add(1)
		go d.dispatchAsyncQueue(d.blockingReplayQueue, d.blockingReplayQueueSlots, asyncQueueBlocking, d.blockingReplaySem)
	}
}

func (d *DualWriter) enqueueAsync(queue chan asyncTask, slots chan struct{}, queueName string, fn func(context.Context)) {
	now := time.Now()
	task := asyncTask{enqueuedAt: now, fn: fn}
	if d.cfg.SecondaryTimeout > 0 {
		task.deadline = now.Add(d.cfg.SecondaryTimeout)
	}

	// Holding mu makes the closed check and channel send atomic with Close.
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return
	}
	select {
	case slots <- struct{}{}:
		d.metrics.AsyncQueueDepth.WithLabelValues(queueName).Inc()
		queue <- task
		d.mu.Unlock()
	default:
		capacity := cap(slots)
		depth := len(slots)
		d.mu.Unlock()
		d.recordAsyncDrop(queueName, asyncDropQueueFull, capacity, depth)
	}
}

func (d *DualWriter) dispatchAsyncQueue(queue <-chan asyncTask, slots chan struct{}, queueName string, sems ...chan struct{}) {
	defer d.dispatchWG.Done()
	for task := range queue {
		if task.isExpired() {
			d.finishAsyncQueueWait(task, slots, queueName)
			d.recordAsyncDrop(queueName, asyncDropExpired, cap(slots), len(slots))
			continue
		}

		for _, sem := range sems {
			sem <- struct{}{}
		}

		d.finishAsyncQueueWait(task, slots, queueName)
		if task.isExpired() {
			d.releaseAsyncSemaphores(sems)
			d.recordAsyncDrop(queueName, asyncDropExpired, cap(slots), len(slots))
			continue
		}

		d.workerWG.Add(1)
		d.metrics.AsyncWorkersActive.WithLabelValues(queueName).Inc()
		go func(task asyncTask) {
			defer func() {
				d.metrics.AsyncWorkersActive.WithLabelValues(queueName).Dec()
				d.releaseAsyncSemaphores(sems)
				d.workerWG.Done()
			}()

			ctx := context.Background()
			cancel := func() {}
			if !task.deadline.IsZero() {
				ctx, cancel = context.WithDeadline(ctx, task.deadline)
			}
			defer cancel()
			task.fn(ctx)
		}(task)
	}
}

func (d *DualWriter) finishAsyncQueueWait(task asyncTask, slots chan struct{}, queueName string) {
	<-slots
	d.metrics.AsyncQueueDepth.WithLabelValues(queueName).Dec()
	queueDelay := time.Since(task.enqueuedAt)
	d.metrics.AsyncQueueDelay.WithLabelValues(queueName).Observe(queueDelay.Seconds())
}

func (t asyncTask) isExpired() bool {
	return !t.deadline.IsZero() && !time.Now().Before(t.deadline)
}

func (d *DualWriter) releaseAsyncSemaphores(sems []chan struct{}) {
	for i := len(sems) - 1; i >= 0; i-- {
		<-sems[i]
	}
}
