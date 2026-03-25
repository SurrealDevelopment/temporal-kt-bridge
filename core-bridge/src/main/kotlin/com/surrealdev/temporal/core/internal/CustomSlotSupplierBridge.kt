package com.surrealdev.temporal.core.internal

import org.slf4j.LoggerFactory
import java.lang.foreign.MemorySegment
import java.lang.foreign.ValueLayout
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import io.temporal.sdkbridge.temporal_sdk_core_c_bridge_h as CoreBridge

/**
 * FFI callback handler for the Core SDK's custom slot supplier.
 */
class CustomSlotSupplierBridge internal constructor(
    private val maximumSlots: Int,
    private val minimumSlots: Int,
) {
    private val logger = LoggerFactory.getLogger(CustomSlotSupplierBridge::class.java)

    /** Incrementing permit IDs. Must be non-zero (Core SDK requirement). */
    private val permitIdGenerator = AtomicLong(1)

    /** Current number of reserved (not yet released) slots. */
    private val activeSlots = AtomicInteger(0)

    /**
     * Pending async reserves, keyed by completionCtx address.
     * Entries are added by [onReserve] and removed by the grant loop or [onCancelReserve].
     */
    private val pendingReserves = ConcurrentHashMap<Long, MemorySegment>()

    /**
     * Volatile PID grant decision, set by the grant loop, read by [onTryReserve].
     */
    @Volatile
    var pidGrantDecision: Boolean = true

    // --- Public methods for the grant loop ---

    /** Returns the current active slot count. */
    fun getActiveSlotCount(): Int = activeSlots.get()

    /** Returns the number of pending reserves waiting to be granted. */
    fun getPendingCount(): Int = pendingReserves.size

    /**
     * Atomically claims one pending reserve and increments activeSlots.
     * Returns the (completionCtx, permitId) pair, or null if nothing to grant or at max capacity.
     *
     * Uses CAS on activeSlots to respect maximumSlots under concurrent tryReserve.
     */
    fun tryClaimNextPending(): GrantClaim? {
        val entry = pendingReserves.entries.firstOrNull() ?: return null

        val current = activeSlots.get()
        if (current >= maximumSlots) return null
        if (!activeSlots.compareAndSet(current, current + 1)) return null

        val ctx = pendingReserves.remove(entry.key)
        if (ctx == null) {
            activeSlots.decrementAndGet()
            return null
        }

        val permitId = permitIdGenerator.getAndIncrement()
        return GrantClaim(ctx, permitId)
    }

    /**
     * Completes an async reserve via FFI. Returns true if accepted by Core.
     * If false, the reservation was cancelled — caller must call [cancelGrant].
     */
    fun completeGrant(
        ctx: MemorySegment,
        permitId: Long,
    ): Boolean = CoreBridge.temporal_core_complete_async_reserve(ctx, permitId)

    /**
     * Acknowledges a cancelled reservation via FFI and decrements activeSlots.
     */
    fun cancelGrant(ctx: MemorySegment) {
        activeSlots.decrementAndGet()
        CoreBridge.temporal_core_complete_async_cancel_reserve(ctx)
    }

    data class GrantClaim(
        val completionCtx: MemorySegment,
        val permitId: Long,
    )

    // --- FFI Callbacks (called from Rust tokio threads) ---

    fun onReserve(
        ctx: MemorySegment,
        completionCtx: MemorySegment,
        userData: MemorySegment,
    ) {
        pendingReserves[completionCtx.address()] = completionCtx
    }

    fun onCancelReserve(
        completionCtx: MemorySegment,
        userData: MemorySegment,
    ) {
        val removed = pendingReserves.remove(completionCtx.address())
        if (removed != null) {
            CoreBridge.temporal_core_complete_async_cancel_reserve(completionCtx)
        }
    }

    fun onTryReserve(
        ctx: MemorySegment,
        userData: MemorySegment,
    ): Long {
        val current = activeSlots.get()
        if (current >= maximumSlots) return 0

        if (current < minimumSlots || pidGrantDecision) {
            if (activeSlots.compareAndSet(current, current + 1)) {
                return permitIdGenerator.getAndIncrement()
            }
        }
        return 0
    }

    fun onMarkUsed(
        ctx: MemorySegment,
        userData: MemorySegment,
    ) {
        // No action needed
    }

    fun onRelease(
        ctx: MemorySegment,
        userData: MemorySegment,
    ) {
        activeSlots.decrementAndGet()
    }

    fun onAvailableSlots(
        availableSlots: MemorySegment,
        userData: MemorySegment,
    ): Boolean {
        val available = (maximumSlots - activeSlots.get()).coerceAtLeast(0).toLong()
        availableSlots.set(ValueLayout.JAVA_LONG, 0, available)
        return true
    }

    fun onFree(selfPtr: MemorySegment) {
        // Drain and cancel-complete any remaining pending reserves.
        // By this point Core should have already cancelled them via CancelReserveGuard drops.
        val remaining = pendingReserves.keys.toList()
        for (address in remaining) {
            val ctx = pendingReserves.remove(address) ?: continue
            try {
                val acknowledged = CoreBridge.temporal_core_complete_async_cancel_reserve(ctx)
                if (!acknowledged) {
                    logger.warn(
                        "Pending reserve at address {} was not in Cancelled state during onFree — possible Arc leak",
                        address,
                    )
                }
            } catch (e: Exception) {
                logger.debug("Failed to cancel-complete pending reserve during shutdown", e)
            }
        }
    }
}
