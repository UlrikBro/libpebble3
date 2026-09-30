package io.rebble.libpebblecommon.datalogging

import co.touchlab.kermit.Logger
import io.rebble.libpebblecommon.SystemAppIDs.SYSTEM_APP_UUID
import io.rebble.libpebblecommon.connection.CustomDataLogging
import io.rebble.libpebblecommon.connection.CustomDataLoggingEvent
import io.rebble.libpebblecommon.connection.CustomDataLoggingResult
import io.rebble.libpebblecommon.connection.CustomDataLoggingSink
import io.rebble.libpebblecommon.connection.WebServices
import io.rebble.libpebblecommon.services.WatchInfo
import io.rebble.libpebblecommon.structmapper.SBytes
import io.rebble.libpebblecommon.structmapper.SUInt
import io.rebble.libpebblecommon.structmapper.StructMappable
import io.rebble.libpebblecommon.util.DataBuffer
import io.rebble.libpebblecommon.util.Endian
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlin.concurrent.atomics.AtomicReference
import kotlin.uuid.Uuid

/** What [io.rebble.libpebblecommon.services.DataLoggingService] sends back for a data item. */
enum class DataLoggingReply {
    ACK,

    /** No reply: the watch times out, keeps the item and re-sends it later. */
    NONE,
}

class Datalogging(
    private val webServices: WebServices,
    private val healthDataProcessor: HealthDataProcessor,
) : CustomDataLogging {
    private val logger = Logger.withTag("Datalogging")

    private val _customData =
        MutableSharedFlow<CustomDataLoggingEvent>(
            extraBufferCapacity = BUFFER_CAPACITY,
            onBufferOverflow = BufferOverflow.DROP_OLDEST,
        )
    override val customData: SharedFlow<CustomDataLoggingEvent> = _customData.asSharedFlow()
    private val sinkRef = AtomicReference<CustomDataLoggingSink?>(null)

    override fun setDataSink(sink: CustomDataLoggingSink?) {
        sinkRef.store(sink)
    }

    /**
     * With a sink registered, custom data goes to it inline, so the ACK is sent only after the
     * sink has handled it; queueing it and ACKing first lost the data on process death or a
     * failed write. A sink that throws, or [CustomDataLoggingResult.RETRY_LATER], means no reply
     * at all, never a NACK (see [CustomDataLoggingResult]). An item is emitted on [customData]
     * once it has been accepted, so collectors do not see the watch's re-sends of a refused one.
     *
     * With no sink, [customData] is the only consumer and nothing can refuse an item, so it is
     * emitted and ACKed as before.
     */
    suspend fun logData(
        sessionId: UByte,
        uuid: Uuid,
        tag: UInt,
        data: ByteArray,
        watchInfo: WatchInfo,
        itemSize: UShort,
        itemsLeft: UInt,
    ): DataLoggingReply {
        // Handle health tags
        if (tag in HealthDataProcessor.HEALTH_TAGS) {
            healthDataProcessor.handleSendDataItems(sessionId, data, itemsLeft)
            return DataLoggingReply.ACK
        }

        // Handle system-app datalogging tags
        if (uuid == SYSTEM_APP_UUID) {
            when (tag) {
                MEMFAULT_CHUNKS_TAG -> {
                    // A single SendDataItems payload can contain multiple items,
                    // each itemSize bytes. Parse each one as a MemfaultChunk.
                    val size = itemSize.toInt()
                    var offset = 0
                    while (offset + size <= data.size) {
                        val itemData = data.copyOfRange(offset, offset + size)
                        val chunk = MemfaultChunk()
                        chunk.fromBytes(DataBuffer(itemData.toUByteArray()))
                        webServices.uploadMemfaultChunk(chunk.bytes.get().toByteArray(), watchInfo)
                        offset += size
                    }
                }

                ANALYTICS_HEARTBEAT_TAG -> {
                    // Fixed-size native_heartbeat_record items (no inner length prefix).
                    val size = itemSize.toInt()
                    if (size <= 0) {
                        logger.w { "Analytics heartbeat with itemSize=$size; ignoring" }
                        return DataLoggingReply.ACK
                    }
                    var offset = 0
                    while (offset + size <= data.size) {
                        val itemData = data.copyOfRange(offset, offset + size)
                        webServices.uploadAnalyticsHeartbeat(itemData, watchInfo)
                        offset += size
                    }
                }
            }
            return DataLoggingReply.ACK
        }
        val event =
            CustomDataLoggingEvent(
                sessionId = sessionId,
                appUuid = uuid,
                tag = tag,
                data = data,
                itemSize = itemSize,
                itemsLeft = itemsLeft,
            )

        val sink = sinkRef.load()
        val reply = if (sink == null) {
            DataLoggingReply.ACK
        } else {
            try {
                when (sink.onData(event)) {
                    CustomDataLoggingResult.ACK -> DataLoggingReply.ACK
                    CustomDataLoggingResult.RETRY_LATER -> DataLoggingReply.NONE
                }
            } catch (e: Throwable) {
                // Only this collector's own cancellation propagates. A stray
                // CancellationException from the sink (a timeout, say) would otherwise end
                // DataLogging for the rest of the connection.
                currentCoroutineContext().ensureActive()
                logger.e(e) { "Sink threw for tag=$tag uuid=$uuid; not replying" }
                DataLoggingReply.NONE
            }
        }
        if (reply == DataLoggingReply.ACK) _customData.tryEmit(event)
        return reply
    }

    fun openSession(
        sessionId: UByte,
        tag: UInt,
        applicationUuid: Uuid,
        itemSize: UShort,
    ) {
        if (tag in HealthDataProcessor.HEALTH_TAGS) {
            healthDataProcessor.handleSessionOpen(sessionId, tag, applicationUuid, itemSize)
        }
    }

    fun closeSession(
        sessionId: UByte,
        tag: UInt,
    ) {
        if (tag in HealthDataProcessor.HEALTH_TAGS) {
            healthDataProcessor.handleSessionClose(sessionId)
        }
    }

    companion object {
        private val MEMFAULT_CHUNKS_TAG: UInt = 86u
        private val ANALYTICS_HEARTBEAT_TAG: UInt = 87u
        private const val BUFFER_CAPACITY = 256
    }
}

class MemfaultChunk : StructMappable() {
    val chunkSize: SUInt = SUInt(m, 0u, Endian.Little)
    val bytes: SBytes = SBytes(m).apply { linkWithSize(chunkSize) }
}
