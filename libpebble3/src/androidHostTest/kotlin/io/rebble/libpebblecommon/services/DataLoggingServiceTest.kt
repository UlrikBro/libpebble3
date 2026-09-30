package io.rebble.libpebblecommon.services

import TestPebbleProtocolHandler
import io.rebble.libpebblecommon.connection.CustomDataLoggingEvent
import io.rebble.libpebblecommon.connection.CustomDataLoggingResult
import io.rebble.libpebblecommon.connection.WebServices
import io.rebble.libpebblecommon.database.dao.HealthDao
import io.rebble.libpebblecommon.database.entity.HealthStatDao
import io.rebble.libpebblecommon.datalogging.Datalogging
import io.rebble.libpebblecommon.datalogging.HealthDataProcessor
import io.rebble.libpebblecommon.di.ConnectionCoroutineScope
import io.rebble.libpebblecommon.di.LibPebbleCoroutineScope
import io.rebble.libpebblecommon.metadata.WatchColor
import io.rebble.libpebblecommon.metadata.WatchHardwarePlatform
import io.rebble.libpebblecommon.packets.DataLoggingIncomingPacket
import io.rebble.libpebblecommon.packets.DataLoggingOutgoingPacket
import io.rebble.libpebblecommon.protocolhelpers.PebblePacket
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import java.lang.reflect.Proxy
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant
import kotlin.uuid.Uuid

private val APP_UUID = Uuid.parse("35221a7e-8e56-421f-a5b2-3d87e515b1df")
private const val APP_TAG = 0x91u

/**
 * The reply is the whole contract with the watch's DataLogging firmware: ACK deletes
 * the data on the watch, silence makes it re-send later, and a NACK must never be sent
 * (the firmware flushes a session on its 21st). These run the real service and
 * [Datalogging] against a fake protocol handler.
 */
class DataLoggingServiceTest {

    private val sent = mutableListOf<PebblePacket>()
    private val handler = TestPebbleProtocolHandler { sent += it }
    private val sinkCalls = mutableListOf<CustomDataLoggingEvent>()

    private val datalogging = Datalogging(
        webServices = unused(),
        healthDataProcessor = HealthDataProcessor(
            LibPebbleCoroutineScope(Dispatchers.Unconfined),
            unused<HealthDao>(),
            unused<HealthStatDao>(),
        ),
    )

    private fun TestScope.startService(): DataLoggingService {
        val service = DataLoggingService(
            handler,
            ConnectionCoroutineScope(backgroundScope.coroutineContext),
            datalogging,
            testScheduler.timeSource,
        )
        service.initialInit()
        runCurrent()
        return service
    }

    /** Delivers [packet] and lets the service finish handling it. */
    private suspend fun TestScope.receive(packet: PebblePacket) {
        handler.receivePacket(packet)
        runCurrent()
    }

    private fun sink(result: suspend (CustomDataLoggingEvent) -> CustomDataLoggingResult) {
        datalogging.setDataSink { event ->
            sinkCalls += event
            result(event)
        }
    }

    private fun sentKinds() = sent.map { it::class.simpleName }

    @Test
    fun data_the_sink_stored_is_acked() = runTest {
        sink { CustomDataLoggingResult.ACK }
        val service = startService()
        service.realInit(watchInfo())
        receive(open(1u))
        sent.clear()

        receive(data(1u, byteArrayOf(1, 2, 3, 4)))

        assertEquals(1, sent.size)
        assertEquals(1u.toUByte(), assertIs<DataLoggingOutgoingPacket.ACK>(sent.single()).sessionId.get())
        assertEquals(listOf(1, 2, 3, 4), sinkCalls.single().data.map { it.toInt() })
    }

    @Test
    fun retry_later_sends_no_reply_at_all() = runTest {
        sink { CustomDataLoggingResult.RETRY_LATER }
        val service = startService()
        service.realInit(watchInfo())
        receive(open(1u))
        sent.clear()

        receive(data(1u))

        assertEquals(1, sinkCalls.size)
        assertEquals(emptyList(), sentKinds())
    }

    @Test
    fun without_a_sink_data_is_acked_and_emitted_on_customData() = runTest {
        // customData is then the only consumer and nothing can refuse an item.
        val emitted = mutableListOf<CustomDataLoggingEvent>()
        backgroundScope.launch(UnconfinedTestDispatcher(testScheduler)) {
            datalogging.customData.collect { emitted += it }
        }
        val service = startService()
        service.realInit(watchInfo())
        receive(open(1u))
        sent.clear()

        receive(data(1u, byteArrayOf(1, 2, 3, 4)))

        assertIs<DataLoggingOutgoingPacket.ACK>(sent.single())
        assertEquals(listOf(1, 2, 3, 4), emitted.single().data.map { it.toInt() })
    }

    @Test
    fun only_items_the_sink_accepted_are_emitted_on_customData() = runTest {
        val emitted = mutableListOf<CustomDataLoggingEvent>()
        backgroundScope.launch(UnconfinedTestDispatcher(testScheduler)) {
            datalogging.customData.collect { emitted += it }
        }
        var calls = 0
        sink { if (calls++ == 0) CustomDataLoggingResult.RETRY_LATER else CustomDataLoggingResult.ACK }
        val service = startService()
        service.realInit(watchInfo())
        receive(open(1u))

        receive(data(1u, byteArrayOf(1, 1, 1, 1)))
        receive(data(1u, byteArrayOf(2, 2, 2, 2)))

        // The refused item comes back from the watch later; collectors see it then, once.
        assertEquals(listOf(2), emitted.map { it.data.first().toInt() })
    }

    @Test
    fun a_throwing_sink_gets_no_reply_and_the_next_item_is_still_handled() = runTest {
        var calls = 0
        sink { if (calls++ == 0) throw IllegalStateException("disk full") else CustomDataLoggingResult.ACK }
        val service = startService()
        service.realInit(watchInfo())
        receive(open(1u))
        sent.clear()

        receive(data(1u))
        assertEquals(emptyList(), sentKinds())

        receive(data(1u))
        assertIs<DataLoggingOutgoingPacket.ACK>(sent.single())
    }

    @Test
    fun a_stray_cancellation_from_the_sink_does_not_end_the_collector() = runTest {
        var calls = 0
        sink { if (calls++ == 0) throw CancellationException("timed out") else CustomDataLoggingResult.ACK }
        val service = startService()
        service.realInit(watchInfo())
        receive(open(1u))
        sent.clear()

        receive(data(1u))
        assertEquals(emptyList(), sentKinds())

        receive(data(1u))
        assertIs<DataLoggingOutgoingPacket.ACK>(sent.single())
    }

    @Test
    fun data_for_an_unknown_session_is_not_answered_and_asks_for_one_reopen() = runTest {
        sink { CustomDataLoggingResult.ACK }
        val service = startService()
        service.realInit(watchInfo())
        sent.clear()

        receive(data(7u))
        receive(data(7u))

        // An ACK would make the watch delete data nobody stored.
        assertEquals(listOf("ReportOpenSessions"), sentKinds())
        assertTrue(sinkCalls.isEmpty())
    }

    @Test
    fun a_reopen_is_asked_for_again_once_the_interval_has_passed() = runTest {
        sink { CustomDataLoggingResult.ACK }
        val service = startService()
        service.realInit(watchInfo())
        sent.clear()

        receive(data(7u))
        advanceTimeBy(31.seconds)
        receive(data(7u))

        assertEquals(listOf("ReportOpenSessions", "ReportOpenSessions"), sentKinds())
    }

    @Test
    fun data_stored_after_the_watch_stopped_waiting_is_not_acked() = runTest {
        sink {
            delay(DataLoggingService.REPLY_WINDOW + 1.seconds)
            CustomDataLoggingResult.ACK
        }
        val service = startService()
        service.realInit(watchInfo())
        receive(open(1u))
        sent.clear()

        handler.receivePacket(data(1u))
        // Explicit: advanceUntilIdle() ignores backgroundScope, where the service runs.
        advanceTimeBy(DataLoggingService.REPLY_WINDOW + 2.seconds)
        runCurrent()

        // Stored, but a late ACK could be taken for a later, larger re-send.
        assertEquals(1, sinkCalls.size)
        assertEquals(emptyList(), sentKinds())
    }

    @Test
    fun the_reply_window_runs_from_arrival_not_from_when_handling_started() = runTest {
        sink { event ->
            delay(if (event.sessionId == 1u.toUByte()) 20.seconds else 10.seconds)
            CustomDataLoggingResult.ACK
        }
        val service = startService()
        service.realInit(watchInfo())
        receive(open(1u))
        receive(open(2u))
        sent.clear()

        launch { handler.receivePacket(data(1u)) }
        advanceTimeBy(1.seconds)
        runCurrent()
        launch { handler.receivePacket(data(2u)) }
        advanceTimeBy(40.seconds)
        runCurrent()

        // Session 2 waited 19 s behind session 1 and took 10 s itself: stored 29 s after
        // the watch sent it, when the watch has stopped waiting. Only session 1 is ACKed.
        assertEquals(2, sinkCalls.size)
        assertEquals(
            listOf(1u.toUByte()),
            sent.map { assertIs<DataLoggingOutgoingPacket.ACK>(it).sessionId.get() },
        )
    }

    @Test
    fun data_stored_within_the_window_is_still_acked() = runTest {
        sink {
            delay(DataLoggingService.REPLY_WINDOW - 1.seconds)
            CustomDataLoggingResult.ACK
        }
        val service = startService()
        service.realInit(watchInfo())
        receive(open(1u))
        sent.clear()

        handler.receivePacket(data(1u))
        // Explicit: advanceUntilIdle() ignores backgroundScope, where the service runs.
        advanceTimeBy(DataLoggingService.REPLY_WINDOW + 2.seconds)
        runCurrent()

        assertIs<DataLoggingOutgoingPacket.ACK>(sent.single())
    }

    @Test
    fun nothing_is_answered_before_init() = runTest {
        sink { CustomDataLoggingResult.ACK }
        val service = startService()

        receive(open(1u))
        receive(data(1u))

        // A NACK here would count towards the watch's global unexpected-NACK limit.
        assertEquals(emptyList(), sentKinds())
        assertTrue(sinkCalls.isEmpty())

        service.realInit(watchInfo())
        receive(open(1u))
        assertEquals(listOf("ReportOpenSessions", "ACK"), sentKinds())
    }

    @Test
    fun close_session_is_never_answered() = runTest {
        sink { CustomDataLoggingResult.ACK }
        val service = startService()
        service.realInit(watchInfo())
        receive(open(1u))
        sent.clear()

        receive(DataLoggingIncomingPacket.CloseSession().apply { sessionId.set(1u) })

        assertEquals(emptyList(), sentKinds())
    }

    @Test
    fun a_nack_is_never_sent() = runTest {
        var calls = 0
        sink {
            when (calls++ % 3) {
                0 -> CustomDataLoggingResult.RETRY_LATER
                1 -> throw IllegalStateException("boom")
                else -> CustomDataLoggingResult.ACK
            }
        }
        val service = startService()
        receive(open(1u))
        receive(data(1u))
        service.realInit(watchInfo())
        receive(open(1u))
        repeat(6) { receive(data(1u)) }
        receive(data(9u))
        receive(DataLoggingIncomingPacket.CloseSession().apply { sessionId.set(1u) })

        assertTrue(sent.none { it is DataLoggingOutgoingPacket.NACK }, "sent ${sentKinds()}")
    }
}

private fun open(id: UByte, uuid: Uuid = APP_UUID, sessionTag: UInt = APP_TAG) =
    DataLoggingIncomingPacket.OpenSession().apply {
        sessionId.set(id)
        applicationUUID.set(uuid)
        timestamp.set(0u)
        tag.set(sessionTag)
        dataItemTypeId.set(2u)
        dataItemSize.set(4u)
    }

private fun data(id: UByte, payload: ByteArray = byteArrayOf(0, 0, 0, 0)) =
    DataLoggingIncomingPacket.SendDataItems().apply {
        sessionId.set(id)
        itemsLeftAfterThis.set(0u)
        crc.set(0u)
        this.payload.set(payload.toUByteArray())
    }

private fun watchInfo(): WatchInfo {
    val firmware = FirmwareVersion(
        stringVersion = "v4.9.0",
        timestamp = Instant.DISTANT_PAST,
        major = 4,
        minor = 9,
        patch = 0,
        suffix = null,
        gitHash = "",
        isRecovery = false,
        isDualSlot = false,
        isSlot0 = false,
    )
    return WatchInfo(
        runningFwVersion = firmware,
        recoveryFwVersion = null,
        platform = WatchHardwarePlatform.UNKNOWN,
        bootloaderTimestamp = Instant.DISTANT_PAST,
        board = "",
        serial = "",
        btAddress = "",
        resourceCrc = 0,
        resourceTimestamp = Instant.DISTANT_PAST,
        language = "",
        languageVersion = 0,
        capabilities = emptySet(),
        isUnfaithful = false,
        healthInsightsVersion = null,
        javascriptVersion = null,
        color = WatchColor.ClassicBlack,
    )
}

/** A dependency the code under test must not touch on these paths. */
private inline fun <reified T : Any> unused(): T =
    Proxy.newProxyInstance(T::class.java.classLoader, arrayOf(T::class.java)) { proxy, method, args ->
        when (method.name) {
            "toString" -> "unused ${T::class.simpleName}"
            "hashCode" -> System.identityHashCode(proxy)
            "equals" -> proxy === args?.get(0)
            else -> error("Unexpected call to ${T::class.simpleName}.${method.name}")
        }
    } as T
