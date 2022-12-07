// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

//==============================================================================
// Exports
//==============================================================================

pub mod fail;
pub mod logging;
pub mod memory;
pub mod network;
pub mod queue;
pub mod timer;
pub mod types;
pub mod watched;
pub use queue::{
    QDesc,
    QResult,
    QToken,
    QType,
};

#[cfg(feature = "liburing")]
pub use liburing;

#[cfg(feature = "libdpdk")]
pub use dpdk_rs as libdpdk;

#[cfg(feature = "libmlx5")]
pub use mlx5_rs as libmlx5;

//==============================================================================
// Traits
//==============================================================================

/// Demikernel Runtime
pub trait Runtime: Clone + Unpin + 'static {}
