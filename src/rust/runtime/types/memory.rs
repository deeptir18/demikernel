// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#![allow(non_camel_case_types)]

//==============================================================================
// Imports
//==============================================================================

use libc::{
    c_void,
    sockaddr,
};

//==============================================================================
// Constants
//==============================================================================

/// Maximum Length for Scatter-Gather Arrays
pub const DEMI_SGARRAY_MAXLEN: usize = 1;

//==============================================================================
// Structures
//==============================================================================

/// Scatter-Gather Array Segment
#[repr(C)]
#[derive(Copy, Clone)]
pub struct demi_sgaseg_t {
    /// Underlying data.
    pub sgaseg_buf: *mut c_void,
    /// Length of underlying data.
    pub sgaseg_len: u32,
}

/// Scatter-Gather Array
#[repr(C)]
#[derive(Copy, Clone)]
pub struct demi_sgarray_t {
    pub sga_buf: *mut c_void,
    pub sga_numsegs: u32,
    pub sga_segs: [demi_sgaseg_t; DEMI_SGARRAY_MAXLEN],
    pub sga_addr: sockaddr,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub union datapath_recovery_info_t {
    /// Pointer to DPDK mbuf
    pub mbuf: *mut c_void,
    /// Pointer to index within memory pool (drivers implementation)
    pub index: u64,
}

pub type MempoolId = u64;
/// Metadata object (read only reference to datapath allocated buffer).
#[repr(C)]
#[derive(Copy, Clone)]
pub struct datapath_metadata_t {
    /// Mempool ID
    pub mempool_id: MempoolId,
    /// Application offset
    pub offset: usize,
    /// Application data length
    pub len: usize,
    /// Recovery information
    pub recovery_info: datapath_recovery_info_t,
}

/// Datapath buffer: Allocated buffer for
#[repr(C)]
pub struct datapath_buffer_t {
    /// Actual data buffer to write into
    pub buffer: *mut c_void,
    /// Length of what has been written so far
    pub data_len: usize,
    /// Maximum length
    pub max_len: usize,
    /// Mempool ID.
    pub mempool_id: MempoolId,
    /// Recovery info
    pub recovery_info: datapath_recovery_info_t,
}
