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
use std::{io::Write};
use anyhow::{
    Error,
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

pub type MempoolId = u64;

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ofed_recovery_info_t {
    /// Pointer to index within memory pool (drivers implementation)
    pub index: u32,
    /// Mempool ID
    pub mempool_id: MempoolId,
}
#[repr(C)]
#[derive(Copy, Clone)]
pub union datapath_recovery_info_t {
    /// Pointer to DPDK mbuf
    pub mbuf: *mut c_void,
    pub ofed_recovery_info: ofed_recovery_info_t,
}

/// Metadata object (read only reference to datapath allocated buffer).
#[repr(C)]
#[derive(Copy, Clone)]
pub struct datapath_metadata_t {
    /// Actual data buffer
    pub buffer: *mut c_void,
    /// Application offset
    pub offset: usize,
    /// Application data length
    pub len: usize,
    /// Recovery information
    pub recovery_info: datapath_recovery_info_t,
}

impl datapath_metadata_t {
    pub fn default() -> Self {
            datapath_metadata_t {
                buffer: std::ptr::null_mut(),
                offset: 0,
                len: 0,
                recovery_info: datapath_recovery_info_t {
                    ofed_recovery_info: ofed_recovery_info_t {
                        index: 0,
                        mempool_id: 0,
                    }
                }
            }
        }
    pub fn as_ref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.buffer as *mut u8, self.len) }
    }

    pub fn data_len(&self) -> usize {
        self.len
    }

    pub fn offset(&self) -> usize {
        self.offset
    }

    pub fn set_data_len_and_offset(&mut self, data_len: usize, offset: usize) -> Result<(), Error> {
        unimplemented!();
    }
}

/// Datapath buffer: Allocated buffer for
#[repr(C)]
// #[derive(Copy)]
pub struct datapath_buffer_t {
    /// Actual data buffer to write into
    pub buffer: *mut c_void,
    /// Length of what has been written so far
    pub data_len: usize,
    /// Maximum length
    pub max_len: usize,
    /// Recovery info
    pub recovery_info: datapath_recovery_info_t,
}

impl datapath_buffer_t {
    pub fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let bytes_to_write = bytes.len();
        let buf_addr = (self.buffer as usize + self.data_len) as *mut u8;
        let mut buf = unsafe { std::slice::from_raw_parts_mut(buf_addr, bytes_to_write) };
        self.data_len += bytes_to_write;
        buf.write(&bytes[0..bytes_to_write]);
        Ok(bytes.len())
    }

    pub fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }

    pub fn as_ref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.buffer as *mut u8, self.data_len) }
    }
}
