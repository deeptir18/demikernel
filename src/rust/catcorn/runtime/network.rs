// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

//==============================================================================
// Imports
//==============================================================================

use super::Mlx5Runtime;
use crate::runtime::{
    memory::Buffer,
    network::{
        consts::RECEIVE_BATCH_SIZE,
        NetworkRuntime,
        PacketBuf,
    },
};
use arrayvec::ArrayVec;

#[cfg(feature = "profiler")]
use crate::timer;

//==============================================================================
// Trait Implementations
//==============================================================================

/// Network Runtime Trait Implementation for DPDK Runtime
impl NetworkRuntime for Mlx5Runtime {
    fn transmit(&self, _buf: Box<dyn PacketBuf>) {
        // 1: inline the packet header
        // 2: for metadata object, get PCI entry directly based on extra offset nd length
        // 3: for cornflakes object, need to do something special
        unimplemented!();
    }

    fn receive(&self) -> ArrayVec<Buffer, RECEIVE_BATCH_SIZE> {
        unimplemented!();
    }
}
