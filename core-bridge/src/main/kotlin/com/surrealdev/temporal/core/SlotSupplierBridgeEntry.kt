package com.surrealdev.temporal.core

import com.surrealdev.temporal.core.internal.CustomSlotSupplierBridge

/**
 * Associates a [CustomSlotSupplierBridge] with its slot type for the grant loop.
 */
data class SlotSupplierBridgeEntry(
    val slotType: String,
    val bridge: CustomSlotSupplierBridge,
)
