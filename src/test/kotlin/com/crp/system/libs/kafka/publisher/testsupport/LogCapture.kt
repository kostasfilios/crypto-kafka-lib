package com.crp.system.libs.kafka.publisher.testsupport

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import org.slf4j.LoggerFactory

/** Captures what the given classes log (logback). Close it to detach. */
class LogCapture(vararg sources: Class<*>) : AutoCloseable {
    private val appender = ListAppender<ILoggingEvent>().apply { start() }
    private val loggers = sources.map { LoggerFactory.getLogger(it) as Logger }

    init {
        loggers.forEach { it.addAppender(appender) }
    }

    val events: List<ILoggingEvent> get() = synchronized(appender) { appender.list.toList() }

    fun lines(level: Level? = null): List<String> = events.filter { level == null || it.level == level }.map { it.formattedMessage }

    fun lines(level: Level, startsWith: String): List<String> = lines(level).filter { it.startsWith(startsWith) }

    override fun close() {
        loggers.forEach { it.detachAppender(appender) }
    }
}
