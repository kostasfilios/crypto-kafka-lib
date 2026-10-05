package com.crp.system.libs.kafka.publisher.core

import ch.qos.logback.classic.Level
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage.INTERNAL
import com.crp.system.libs.kafka.publisher.spring.ChannelSettings
import com.crp.system.libs.kafka.publisher.testsupport.FakeRecordSender
import com.crp.system.libs.kafka.publisher.testsupport.Harness
import com.crp.system.libs.kafka.publisher.testsupport.LogCapture
import com.crp.system.libs.kafka.publisher.testsupport.SampleEvent
import com.crp.system.libs.kafka.publisher.testsupport.Threads
import com.crp.system.libs.kafka.publisher.testsupport.awaitFailure
import com.crp.system.libs.kafka.publisher.testsupport.awaitResult
import com.crp.system.libs.kafka.publisher.testsupport.eventually
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

/** Review finding M7: a lane worker that dies is restarted once; after that its queued records fail as INTERNAL. */
class LaneSupervisionTest {
    private val cleanups = mutableListOf<() -> Unit>()

    @AfterEach
    fun cleanUp() = cleanups.reversed().forEach { it() }

    private class Poisoned : Error("toString exploded")

    /** A task whose exception cannot even be printed: the lane's per-task guard throws while logging it, so the worker dies. */
    private fun poison() = Runnable {
        throw object : RuntimeException() {
            override fun toString(): String = throw Poisoned()
        }
    }

    private class RecordingTask : LaneTask {
        val ran = AtomicBoolean()
        val abandoned = CopyOnWriteArrayList<Pair<PublishFailureStage, Throwable?>>()
        override fun run() = ran.set(true)
        override fun abandon(stage: PublishFailureStage, cause: Throwable?) {
            abandoned += stage to cause
        }
    }

    private fun lane(name: String): PublishLane = PublishLane(name, 10).also { lane ->
        cleanups += { lane.stopAccepting(); lane.awaitDrained(System.nanoTime() + 2_000_000_000) }
    }

    private fun liveWorker(name: String): Thread = Thread.getAllStackTraces().keys.single { it.name == name && it.isAlive }

    @Test
    fun `a worker that dies is restarted once, with an ERROR, and keeps serving the queue`() {
        LogCapture(PublishLane::class.java).use { logs ->
            val lane = lane("phoenix-publisher-0")
            val first = liveWorker("phoenix-publisher-0")
            val served = CountDownLatch(1)

            lane.tryEnqueue(poison())
            lane.tryEnqueue { served.countDown() }

            assertThat(served.await(5, TimeUnit.SECONDS)).isTrue()
            first.join(5_000)
            assertThat(first.isAlive).isFalse()
            val second = liveWorker("phoenix-publisher-0")
            assertThat(second).isNotSameAs(first)
            assertThat(second.isDaemon).isTrue()
            assertThat(logs.lines(Level.ERROR)).containsExactly(
                "kafka_publisher_lane_restarted lane=phoenix-publisher-0 cause=${Poisoned::class.java.name}: toString exploded",
            )
        }
    }

    @Test
    fun `a worker that dies twice gives its queued tasks up as INTERNAL and stops accepting`() {
        LogCapture(PublishLane::class.java).use { logs ->
            val lane = lane("doomed-publisher-0")
            val gate = CountDownLatch(1)
            val gated = CountDownLatch(1)
            lane.tryEnqueue(poison()) // first death: restarted
            lane.tryEnqueue { gated.countDown(); gate.await(5, TimeUnit.SECONDS) } // the restarted worker waits here
            assertThat(gated.await(5, TimeUnit.SECONDS)).isTrue()
            lane.tryEnqueue(poison()) // second death
            val queued = List(3) { RecordingTask() }.onEach { assertThat(lane.tryEnqueue(it)).isTrue() }
            val foreignRan = AtomicBoolean(false)
            assertThat(lane.tryEnqueue { foreignRan.set(true) }).isTrue() // not a record: dropped, not counted

            gate.countDown()

            eventually { queued.all { it.abandoned.isNotEmpty() } }
            assertThat(queued).allMatch { task -> !task.ran.get() && task.abandoned.single().first == INTERNAL && task.abandoned.single().second is Poisoned }
            assertThat(lane.tryEnqueue {}).isFalse()
            assertThat(foreignRan.get()).isFalse()
            assertThat(logs.lines(Level.ERROR).last())
                .isEqualTo("kafka_publisher_lane_dead lane=doomed-publisher-0 abandoned=3 cause=${Poisoned::class.java.name}: toString exploded")
            assertThat(Threads.awaitGone("doomed-publisher-0")).isEmpty()
        }
    }

    @Test
    fun `awaitDrained follows a worker that was restarted while it waited`() {
        LogCapture(PublishLane::class.java).use {
            val lane = lane("follow-publisher-0")
            val gate = CountDownLatch(1)
            val gated = CountDownLatch(1)
            lane.tryEnqueue { gated.countDown(); gate.await(5, TimeUnit.SECONDS) }
            assertThat(gated.await(5, TimeUnit.SECONDS)).isTrue()
            lane.tryEnqueue(poison()) // the first worker dies after the gate
            val replacementBusy = CountDownLatch(1)
            val releaseReplacement = CountDownLatch(1)
            val drainedByReplacement = AtomicBoolean(false)
            lane.tryEnqueue { replacementBusy.countDown(); releaseReplacement.await(10, TimeUnit.SECONDS); drainedByReplacement.set(true) }
            lane.stopAccepting()
            val waiting = Thread { lane.awaitDrained(System.nanoTime() + TimeUnit.SECONDS.toNanos(10)) }.apply { start() }
            eventually { waiting.state == Thread.State.TIMED_WAITING } // joining the first worker

            gate.countDown()
            assertThat(replacementBusy.await(5, TimeUnit.SECONDS)).isTrue() // the first worker died; its replacement works
            waiting.join(200)
            assertThat(waiting.isAlive).describedAs("still waiting for the restarted worker").isTrue()

            releaseReplacement.countDown()
            waiting.join(10_000)
            assertThat(waiting.isAlive).isFalse()
            assertThat(drainedByReplacement.get()).isTrue() // it returned only after the replacement drained the queue
            assertThat(Thread.getAllStackTraces().keys.filter { it.name == "follow-publisher-0" && it.isAlive }).isEmpty()
        }
    }

    @Test
    fun `publishes queued on a lane that died for good fail as INTERNAL instead of hanging`() {
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val h = Harness(ChannelSettings(enabled = true, lanes = 1, queueCapacity = 16), FakeRecordSender(FakeRecordSender.stuckFor("first", entered, release)))
        cleanups += { release.countDown(); h.close() }
        val first = h.publisher.publishWithResult("t1", "first", SampleEvent("x", "1"))
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue()
        val lane = h.lanes.forKey("first")
        lane.tryEnqueue(poison())
        lane.tryEnqueue(poison())
        val stranded = listOf("b", "c").map { h.publisher.publishWithResult("t1", it, SampleEvent("x", "1")) }

        release.countDown()

        assertThat(first.awaitResult().key).isEqualTo("first")
        stranded.map { it.awaitFailure() }.forEach { failure ->
            assertThat(failure.stage).isEqualTo(INTERNAL)
            assertThat(failure.attempt).isEqualTo(0)
            assertThat(failure.cause).isInstanceOf(Poisoned::class.java)
        }
        assertThat(h.publisher.publishWithResult("t1", "late", SampleEvent("x", "1")).awaitFailure().stage)
            .isEqualTo(PublishFailureStage.QUEUE_FULL) // the dead lane takes nothing more
    }
}
