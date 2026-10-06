package com.crp.system.libs.kafka.publisher.core

import ch.qos.logback.classic.Level
import com.crp.system.libs.kafka.publisher.api.PublishFailureStage.CHANNEL_UNAVAILABLE
import com.crp.system.libs.kafka.publisher.spring.ChannelSettings
import com.crp.system.libs.kafka.publisher.spring.OrderingMode
import com.crp.system.libs.kafka.publisher.testsupport.FakeRecordSender
import com.crp.system.libs.kafka.publisher.testsupport.Harness
import com.crp.system.libs.kafka.publisher.testsupport.LogCapture
import com.crp.system.libs.kafka.publisher.testsupport.RecordingTask
import com.crp.system.libs.kafka.publisher.testsupport.SampleEvent
import com.crp.system.libs.kafka.publisher.testsupport.Threads
import com.crp.system.libs.kafka.publisher.testsupport.awaitResult
import com.crp.system.libs.kafka.publisher.testsupport.eventually
import com.crp.system.libs.kafka.publisher.testsupport.offerPastTheAcceptingCheck
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class PublishLanesTest {
    private val cleanups = mutableListOf<() -> Unit>()

    @AfterEach
    fun cleanUp() = cleanups.reversed().forEach { it() }

    private fun lanes(name: String, count: Int, capacity: Int, ordering: OrderingMode = OrderingMode.PER_KEY): PublishLanes =
        PublishLanes(name, count, capacity, ordering).also { lanes -> cleanups += { lanes.stopAccepting(); lanes.awaitDrained(System.nanoTime() + 2_000_000_000) } }

    /** Two keys whose hashes pick different lanes out of [count]. */
    private fun keysOnDifferentLanes(count: Int): Pair<String, String> {
        val first = "player-1"
        val second = (2..1_000).map { "player-$it" }.first { Math.floorMod(it.hashCode(), count) != Math.floorMod(first.hashCode(), count) }
        return first to second
    }

    @Test
    fun `PER_KEY keeps each key's order while another key's lane is stuck`() {
        val (stuckKey, freeKey) = keysOnDifferentLanes(2)
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        cleanups += { release.countDown() }
        val sender = FakeRecordSender { record, index, onOutcome ->
            if (record.key == stuckKey && (record.payload.contains("\"seq\":1"))) FakeRecordSender.stuck(entered, release)(record, index, onOutcome)
            else FakeRecordSender.deliver()(record, index, onOutcome)
        }
        val h = Harness(ChannelSettings(enabled = true, lanes = 2, queueCapacity = 100), sender).also { cleanups += { it.close() } }

        h.publisher.publish("t1", stuckKey, SampleEvent("x", stuckKey, seq = 1))
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue()
        val stuckRest = (2..5).map { h.publisher.publishWithResult("t1", stuckKey, SampleEvent("x", stuckKey, seq = it)) }
        val free = (1..5).map { h.publisher.publishWithResult("t1", freeKey, SampleEvent("x", freeKey, seq = it)) }

        free.forEach { it.awaitResult() } // the free key's lane is not blocked by the stuck one
        assertThat(stuckRest).noneMatch { it.isDone }
        assertThat(h.sender.calls.filter { it.record.key == stuckKey }).hasSize(1)

        release.countDown()
        stuckRest.forEach { it.awaitResult() }
        assertThat(seqs(h, stuckKey)).containsExactly(1, 2, 3, 4, 5)
        assertThat(seqs(h, freeKey)).containsExactly(1, 2, 3, 4, 5)
        assertThat(h.sender.calls.filter { it.record.key == stuckKey }.map { it.thread }.distinct()).hasSize(1)
        assertThat(h.sender.calls.filter { it.record.key == freeKey }.map { it.thread }.distinct()).hasSize(1)
    }

    private fun seqs(h: Harness, key: String): List<Int> =
        h.sender.calls.filter { it.record.key == key }.map { Regex("\"seq\":(\\d+)").find(it.record.payload)!!.groupValues[1].toInt() }

    @Test
    fun `NONE spreads one key across all lanes round-robin`() {
        val h = Harness(ChannelSettings(enabled = true, lanes = 4, queueCapacity = 400, ordering = OrderingMode.NONE)).also { cleanups += { it.close() } }
        (1..40).map { h.publisher.publishWithResult("t1", "same-key", SampleEvent("x", "1", it)) }.forEach { it.awaitResult() }

        val perLane = h.sender.calls.groupingBy { it.thread }.eachCount()
        assertThat(perLane.keys).containsExactlyInAnyOrderElementsOf((0..3).map { "${h.channel}-publisher-$it" })
        assertThat(perLane.values).allMatch { it == 10 }
    }

    @Test
    fun `PER_KEY keeps one key on one lane, and a null key goes round-robin`() {
        val lanes = lanes("perkey", 4, 40)
        assertThat((1..20).map { lanes.forKey("player-7") }.distinct()).hasSize(1)
        assertThat((1..8).map { lanes.forKey(null) }.distinct()).hasSize(4)
        val noneLanes = lanes("none", 4, 40, OrderingMode.NONE)
        assertThat((1..8).map { noneLanes.forKey("player-7") }.distinct()).hasSize(4)
    }

    @Test
    fun `PER_KEY picks the lane by the key's hash`() {
        val lanes = lanes("hash", 3, 30)
        val byLane = (1..300).map { "k$it" }.groupBy { lanes.forKey(it).name }
        byLane.forEach { (lane, keys) -> assertThat(keys).allMatch { lane == "hash-publisher-${Math.floorMod(it.hashCode(), 3)}" } }
        assertThat(lanes.forKey("negative-hash-key-${Int.MIN_VALUE}").name).startsWith("hash-publisher-")
    }

    @Test
    fun `the total capacity is split evenly across the lanes, rounding up`() {
        val lanes = lanes("split", 3, 10)
        val release = CountDownLatch(1)
        cleanups += { release.countDown() }
        val running = CountDownLatch(3)
        val laneList = (0 until 300).map { lanes.forKey(null) }.distinct()
        assertThat(laneList).hasSize(3)
        laneList.forEach { lane -> lane.tryEnqueue { running.countDown(); release.await(10, TimeUnit.SECONDS) } }
        assertThat(running.await(5, TimeUnit.SECONDS)).isTrue() // every worker is busy

        val accepted = laneList.map { lane -> generateSequence { lane.tryEnqueue {} }.takeWhile { it }.count() }
        assertThat(accepted).containsExactly(4, 4, 4) // ceil(10 / 3)
        assertThat(lanes.queued()).isEqualTo(12)
    }

    @Test
    fun `a lane runs its tasks in FIFO order on one named daemon thread`() {
        // created from a non-daemon thread, so the worker is a daemon only because the lane makes it one
        var created: PublishLane? = null
        Thread { created = PublishLane("fifo-publisher-0", 100) }.apply { isDaemon = false; start(); join(5_000) }
        val lane = created!!
        cleanups += { lane.stopAccepting(); lane.awaitDrained(System.nanoTime() + 2_000_000_000) }
        val seen = CopyOnWriteArrayList<Pair<Int, String>>()
        val done = CountDownLatch(50)
        repeat(50) { i -> assertThat(lane.tryEnqueue { seen += i to Thread.currentThread().name; done.countDown() }).isTrue() }
        assertThat(done.await(5, TimeUnit.SECONDS)).isTrue()
        assertThat(seen.map { it.first }).isEqualTo((0 until 50).toList())
        assertThat(seen.map { it.second }.distinct()).containsExactly("fifo-publisher-0")
        assertThat(Thread.getAllStackTraces().keys.single { it.name == "fifo-publisher-0" }.isDaemon).isTrue()
    }

    @Test
    fun `a throwing task does not kill the lane, and the defect is logged`() {
        LogCapture(PublishLane::class.java).use { logs ->
            val lane = PublishLane("survivor-publisher-0", 10)
            cleanups += { lane.stopAccepting(); lane.awaitDrained(System.nanoTime() + 2_000_000_000) }
            val after = CountDownLatch(1)
            lane.tryEnqueue { throw IllegalStateException("task defect") }
            lane.tryEnqueue { throw StackOverflowError() }
            lane.tryEnqueue { after.countDown() }
            assertThat(after.await(5, TimeUnit.SECONDS)).isTrue()
            assertThat(logs.lines(Level.ERROR)).containsExactly(
                "kafka_publisher_lane_task_failed lane=survivor-publisher-0: java.lang.IllegalStateException: task defect",
                "kafka_publisher_lane_task_failed lane=survivor-publisher-0: java.lang.StackOverflowError",
            )
        }
    }

    @Test
    fun `an idle lane wakes from its empty poll quietly and exits once stopped`() {
        LogCapture(PublishLane::class.java).use { logs ->
            val lane = PublishLane("idle-publisher-0", 4)
            val worker = Thread.getAllStackTraces().keys.single { it.name == "idle-publisher-0" }
            eventually { worker.state == Thread.State.TIMED_WAITING } // parked in its 100 ms poll with nothing to do

            lane.stopAccepting()
            lane.awaitDrained(System.nanoTime() + 5_000_000_000) // the empty poll times out, then the loop sees the stop

            assertThat(worker.isAlive).isFalse()
            assertThat(logs.lines(Level.ERROR)).isEmpty() // an empty poll is not a task
        }
    }

    @Test
    fun `stopping refuses new tasks, drains the queued ones, and the worker exits`() {
        val lane = PublishLane("drain-publisher-0", 10)
        val gate = CountDownLatch(1)
        val ran = AtomicInteger()
        lane.tryEnqueue { gate.await(5, TimeUnit.SECONDS); ran.incrementAndGet() }
        repeat(3) { lane.tryEnqueue { ran.incrementAndGet() } }

        lane.stopAccepting()
        assertThat(lane.tryEnqueue { ran.incrementAndGet() }).isFalse()
        assertThat(lane.enqueueWaiting({ ran.incrementAndGet() }, Duration.ofMillis(10))).isFalse()
        gate.countDown()
        lane.awaitDrained(System.nanoTime() + 5_000_000_000)

        assertThat(ran.get()).isEqualTo(4)
        assertThat(Threads.named("drain-publisher-0")).isEmpty()
    }

    @Test
    fun `a stopped lane refuses a waiting offer at once, even when it is full`() {
        val lane = PublishLane("stopped-full-publisher-0", 1)
        val release = CountDownLatch(1)
        cleanups += { release.countDown() }
        val running = CountDownLatch(1)
        lane.tryEnqueue { running.countDown(); release.await(30, TimeUnit.SECONDS) }
        assertThat(running.await(5, TimeUnit.SECONDS)).isTrue()
        assertThat(lane.tryEnqueue {}).isTrue() // the one slot
        lane.stopAccepting()

        val started = System.nanoTime()
        assertThat(lane.enqueueWaiting({}, Duration.ofSeconds(5))).isFalse()
        assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)).isLessThan(1_000)
    }

    @Test
    fun `awaitDrained gives up at the deadline when the worker is stuck`() {
        val lane = PublishLane("stuck-publisher-0", 10)
        val release = CountDownLatch(1)
        cleanups += { release.countDown() }
        val running = CountDownLatch(1)
        lane.tryEnqueue { running.countDown(); release.await(30, TimeUnit.SECONDS) }
        assertThat(running.await(5, TimeUnit.SECONDS)).isTrue()
        lane.stopAccepting()

        val started = System.nanoTime()
        lane.awaitDrained(System.nanoTime() + 200_000_000)
        val waitedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)

        assertThat(waitedMillis).isBetween(150L, 5_000L)
        assertThat(Threads.named("stuck-publisher-0")).isNotEmpty()
    }

    @Test
    fun `awaitDrained with a passed deadline returns at once`() {
        val lane = PublishLane("late-publisher-0", 10)
        val release = CountDownLatch(1)
        cleanups += { release.countDown() }
        lane.tryEnqueue { release.await(30, TimeUnit.SECONDS) }
        lane.stopAccepting()
        val started = System.nanoTime()
        lane.awaitDrained(System.nanoTime() - 1_000_000_000)
        assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)).isLessThan(1_000)
    }

    @Test
    fun `awaitDrained keeps the closing thread's interrupt status`() {
        val lane = PublishLane("interrupt-publisher-0", 10)
        val release = CountDownLatch(1)
        cleanups += { release.countDown() }
        lane.tryEnqueue { release.await(30, TimeUnit.SECONDS) }
        lane.stopAccepting()
        Thread.currentThread().interrupt()
        lane.awaitDrained(System.nanoTime() + 10_000_000_000)
        assertThat(Thread.interrupted()).isTrue()
    }

    @Test
    fun `an offer that races a stop is handed back when the worker has already exited`() {
        val lane = PublishLane("race-publisher-0", 10)
        lane.stopAccepting()
        eventually { Threads.named("race-publisher-0").isEmpty() }
        // the task got past the accepting check just before the stop: the queue takes it, nobody would run it
        val raced = Runnable {}
        val queueField = PublishLane::class.java.getDeclaredField("queue").apply { isAccessible = true }
        @Suppress("UNCHECKED_CAST")
        val queue = queueField.get(lane) as java.util.concurrent.BlockingQueue<Runnable>
        assertThat(queue.offer(raced)).isTrue()
        // the admission check sees the stop and takes it back
        val admitted = PublishLane::class.java.getDeclaredMethod("admitted", Runnable::class.java, Boolean::class.javaPrimitiveType)
            .apply { isAccessible = true }.invoke(lane, raced, true) as Boolean
        assertThat(admitted).isFalse()
        assertThat(queue).isEmpty()

        // the opposite race: the draining worker already took the task, so it will run and the caller must not report it
        val alreadyTaken = Runnable {}
        val admittedTaken = PublishLane::class.java.getDeclaredMethod("admitted", Runnable::class.java, Boolean::class.javaPrimitiveType)
            .apply { isAccessible = true }.invoke(lane, alreadyTaken, true) as Boolean
        assertThat(admittedTaken).isTrue()
    }

    @Test
    fun `halting stops the worker before it takes another task, which stays queued for the caller to take`() {
        val lane = PublishLane("halt-publisher-0", 10)
        cleanups += { lane.halt(); lane.awaitDrained(System.nanoTime() + 2_000_000_000) }
        val gate = CountDownLatch(1)
        cleanups += { gate.countDown() }
        val running = CountDownLatch(1)
        lane.tryEnqueue { running.countDown(); gate.await(10, TimeUnit.SECONDS) }
        assertThat(running.await(5, TimeUnit.SECONDS)).isTrue()
        val queued = List(3) { RecordingTask() }.onEach { assertThat(lane.tryEnqueue(it)).isTrue() }

        lane.halt()
        gate.countDown() // the worker finishes the task it is running, and must not take another
        lane.awaitDrained(System.nanoTime() + 5_000_000_000)

        assertThat(Threads.named("halt-publisher-0")).isEmpty() // it left
        assertThat(queued).noneMatch { it.ran.get() || it.abandoned.isNotEmpty() } // and left them alone
        assertThat(lane.takeRemaining()).containsExactlyElementsOf(queued)
        assertThat(lane.tryEnqueue {}).isFalse()
    }

    @Test
    fun `haltAndTakeRemaining halts every lane, then returns what each of them still had queued`() {
        val lanes = lanes("cutoff", 2, 20)
        val gate = CountDownLatch(1)
        cleanups += { gate.countDown() }
        val running = CountDownLatch(2)
        val laneList = (0 until 100).map { lanes.forKey(null) }.distinct()
        assertThat(laneList).hasSize(2)
        laneList.forEach { lane -> lane.tryEnqueue { running.countDown(); gate.await(10, TimeUnit.SECONDS) } }
        assertThat(running.await(5, TimeUnit.SECONDS)).isTrue()
        val queued = laneList.flatMap { lane -> List(2) { RecordingTask() }.onEach { assertThat(lane.tryEnqueue(it)).isTrue() } }

        val remaining = lanes.haltAndTakeRemaining()
        gate.countDown()
        lanes.awaitDrained(System.nanoTime() + 5_000_000_000)

        assertThat(remaining).containsExactlyInAnyOrderElementsOf(queued)
        assertThat(queued).noneMatch { it.ran.get() }
        assertThat(laneList).noneMatch { it.tryEnqueue {} }
    }

    @Test
    fun `a worker that polls a task just as its lane halts fails it as CHANNEL_UNAVAILABLE and does not run it`() {
        // Each worker must still be polling when the halt comes and each task must land right after it. When this thread
        // is held up for the rest of a 100 ms poll, a worker leaves first and the attempt is made again.
        repeat(20) { attempt ->
            val name = "refuse$attempt-${System.nanoTime()}"
            val lanes = PublishLanes(name, 2, 4, OrderingMode.NONE)
            val workers = (0..1).map { n -> Thread.getAllStackTraces().keys.single { it.name == "$name-publisher-$n" } }
            workers.forEach { worker -> eventually { worker.state == Thread.State.TIMED_WAITING } } // polling an empty queue
            val laneList = (0..1).map { lanes.forKey(null) }
            val tasks = List(2) { RecordingTask() }

            val remaining = lanes.haltAndTakeRemaining()
            laneList.zip(tasks).forEach { (lane, task) -> lane.offerPastTheAcceptingCheck(task) }
            lanes.awaitDrained(System.nanoTime() + 5_000_000_000)

            if (tasks.all { it.abandoned.isNotEmpty() }) {
                assertThat(remaining).isEmpty()
                assertThat(tasks).allMatch { task -> !task.ran.get() && task.abandoned.single() == (CHANNEL_UNAVAILABLE to null) }
                assertThat(Threads.awaitGone("$name-publisher-")).isEmpty()
                return
            }
        }
        fail<Unit>("no attempt had both workers poll the task that was offered after the halt")
    }

    @Test
    fun `queue depth sums every lane`() {
        val lanes = lanes("depth", 2, 4)
        val release = CountDownLatch(1)
        cleanups += { release.countDown() }
        val busy = CountDownLatch(2)
        val laneList = listOf(lanes.forKey(null), lanes.forKey(null))
        laneList.forEach { it.tryEnqueue { busy.countDown(); release.await(10, TimeUnit.SECONDS) } }
        assertThat(busy.await(5, TimeUnit.SECONDS)).isTrue()
        laneList[0].tryEnqueue {}
        laneList[1].tryEnqueue {}
        laneList[1].tryEnqueue {}
        assertThat(lanes.queued()).isEqualTo(3)
    }
}
