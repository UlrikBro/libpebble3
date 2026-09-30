package io.rebble.libpebblecommon.services

import co.touchlab.kermit.Logger
import io.rebble.libpebblecommon.connection.PebbleProtocolHandler
import io.rebble.libpebblecommon.datalogging.DataLoggingReply
import io.rebble.libpebblecommon.datalogging.Datalogging
import io.rebble.libpebblecommon.di.ConnectionCoroutineScope
import io.rebble.libpebblecommon.packets.DataLoggingIncomingPacket
import io.rebble.libpebblecommon.packets.DataLoggingOutgoingPacket
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.filterIsInstance
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeMark
import kotlin.time.TimeSource
import kotlin.uuid.Uuid

/**
 * Replies only with ACK, or not at all; never NACK. The firmware (PebbleOS dls_endpoint.c)
 * flushes a session's whole buffer on the 21st NACK since its last data ACK, and counts
 * NACKs to an idle or opening session towards a global limit after which it no longer
 * reopens NACKed sessions until reboot. An unanswered message times out after 30 s
 * without touching either counter, and the watch keeps the data and re-sends it later.
 *
 * CloseSession is never answered: the watch deletes the session as it sends the close and
 * waits for no reply, so a reply could only land on a new session that reused the id.
 *
 * Nor is anything answered late. Once the watch has stopped waiting, an ACK is at best
 * ignored and at worst taken as the ACK for a later re-send of the session, which may
 * carry more items than were stored here.
 */
class DataLoggingService(
    private val protocolHandler: PebbleProtocolHandler,
    private val scope: ConnectionCoroutineScope,
    private val datalogging: Datalogging,
    private val timeSource: TimeSource = TimeSource.Monotonic,
) : ProtocolService {
    private var watchInfo: WatchInfo? = null
    private var acceptSessions = false
    private var lastReopenRequest: TimeMark? = null

    companion object {
        private val logger = Logger.withTag("DataLoggingService")

        /** Long enough for a reopen pass to finish, so a burst of unknown-session data costs one report. */
        private val REOPEN_REQUEST_INTERVAL = 30.seconds

        /**
         * The watch waits 30 s for a reply (dls_endpoint.c). An item not handled within
         * this long of arriving is not answered, leaving margin for the ACK's transit.
         * The data is already stored, so the watch's re-send is de-duplicated by the sink.
         */
        internal val REPLY_WINDOW = 25.seconds
    }

    private val sessions = mutableMapOf<UByte, DataLoggingSession>()

    private suspend fun send(packet: DataLoggingOutgoingPacket) {
        protocolHandler.send(packet)
    }

    /** An empty report makes the watch reopen every session it holds (prv_handle_report_cmd). */
    private suspend fun requestReopen() {
        val last = lastReopenRequest
        if (last != null && last.elapsedNow() < REOPEN_REQUEST_INTERVAL) return
        lastReopenRequest = timeSource.markNow()
        send(DataLoggingOutgoingPacket.ReportOpenSessions(emptyList()))
    }

    suspend fun realInit(info: WatchInfo) {
        watchInfo = info
        acceptSessions = true
        send(DataLoggingOutgoingPacket.ReportOpenSessions(emptyList()))
    }

    fun initialInit() {
        protocolHandler.inboundMessages
            .filterIsInstance<DataLoggingIncomingPacket>()
            // Stamped as they arrive and buffered, so the reply window runs from when the
            // watch sent an item rather than from when this collector reached it behind
            // another session's persist. Unbounded in type only: the watch keeps at most one
            // unanswered message per session.
            .map { it to timeSource.markNow() }
            .buffer(Channel.UNLIMITED)
            .onEach { (packet, arrived) ->
                when (packet) {
                    is DataLoggingIncomingPacket.OpenSession -> {
                        val id = packet.sessionId.get()
                        val tag = packet.tag.get()
                        val applicationUuid = packet.applicationUUID.get()
                        val itemSize = packet.dataItemSize.get()
                        logger.d { "Session opened: $id tag: $tag (accepted: $acceptSessions)" }
                        sessions[id] = DataLoggingSession(id, tag, applicationUuid, itemSize)
                        datalogging.openSession(id, tag, applicationUuid, itemSize)
                        // Before realInit the open times out on the watch, and realInit's report
                        // makes it reopen.
                        if (acceptSessions) send(DataLoggingOutgoingPacket.ACK(id))
                    }
                    is DataLoggingIncomingPacket.SendDataItems -> {
                        val id = packet.sessionId.get()
                        if (!acceptSessions) {
                            logger.d { "Data for session $id before init; not replying" }
                            return@onEach
                        }
                        val session = sessions[id]
                        if (session == null) {
                            // Typically a session the watch opened on an earlier connection.
                            // An ACK would discard data nobody stored.
                            logger.w { "Data for unknown session $id; not replying, asking the watch to reopen" }
                            requestReopen()
                            return@onEach
                        }
                        val info = watchInfo
                        if (info == null) {
                            logger.e { "watch info is null; not replying" }
                            return@onEach
                        }
                        val reply = datalogging.logData(
                            sessionId = id,
                            uuid = session.uuid,
                            tag = session.tag,
                            data = packet.payload.get().toByteArray(),
                            watchInfo = info,
                            itemSize = session.itemSize,
                            itemsLeft = packet.itemsLeftAfterThis.get(),
                        )
                        val elapsed = arrived.elapsedNow()
                        when {
                            reply == DataLoggingReply.ACK && elapsed < REPLY_WINDOW ->
                                send(DataLoggingOutgoingPacket.ACK(id))
                            reply == DataLoggingReply.ACK ->
                                logger.w { "Data for session $id took $elapsed to handle; too late to ACK, the watch will re-send it" }
                            else ->
                                logger.w { "Not replying to data for session $id; the watch will re-send it" }
                        }
                    }
                    is DataLoggingIncomingPacket.CloseSession -> {
                        val id = packet.sessionId.get()
                        val session = sessions[id]
                        logger.d { "Session closed: $id" }
                        if (session != null) {
                            datalogging.closeSession(id, session.tag)
                        }
                        sessions.remove(id)
                    }
                    // A Timeout reports that the watch gave up waiting for us; nothing to answer.
                    is DataLoggingIncomingPacket.Timeout,
                    is DataLoggingIncomingPacket.SendEnabledResponse -> Unit
                }
            }
            .launchIn(scope)
    }
}

data class DataLoggingSession(
    val id: UByte,
    val tag: UInt,
    val uuid: Uuid,
    val itemSize: UShort,
)
