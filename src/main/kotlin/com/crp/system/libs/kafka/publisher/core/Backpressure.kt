package com.crp.system.libs.kafka.publisher.core

import java.time.Duration

/** A full lane drops the record (reported as QUEUE_FULL). The caller never waits. */
internal object DropWhenFull : BackpressurePolicy {
    override fun admit(lane: PublishLane, task: Runnable) = lane.tryEnqueue(task)
}

/** A full lane makes the caller send. Same-key order can break here (AD3). */
internal object CallerRunsWhenFull : BackpressurePolicy {
    override fun admit(lane: PublishLane, task: Runnable): Boolean {
        if (!lane.tryEnqueue(task)) task.run()
        return true
    }
}

/** A full lane makes the caller wait up to [timeout], then the record is reported QUEUE_FULL. */
internal class BlockWithTimeout(private val timeout: Duration) : BackpressurePolicy {
    override fun admit(lane: PublishLane, task: Runnable) = lane.enqueueWaiting(task, timeout)
}
