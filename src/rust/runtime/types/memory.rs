// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#![allow(non_camel_case_types)]

//==============================================================================
// Imports
//==============================================================================

use crate::runtime::fail::Fail;
use libc::{
    c_void,
    sockaddr,
};
use std::io::Write;
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

pub type MempoolID = u64;

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ofed_recovery_info_t {
    /// Pointer to index within memory pool (drivers implementation)
    pub index: usize,
    /// Mempool pointer
    pub mempool: *mut ::std::os::raw::c_void,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub union datapath_recovery_info_t {
    /// Pointer to DPDK mbuf
    pub mbuf: *mut c_void,
    pub ofed_recovery_info: ofed_recovery_info_t,
}

impl std::fmt::Debug for datapath_recovery_info_t {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        unsafe {
            match self {
                datapath_recovery_info_t { ofed_recovery_info } => f
                    .debug_struct("ofed_recovery_info")
                    .field("index", &ofed_recovery_info.index)
                    .field("mempool", &ofed_recovery_info.mempool)
                    .finish(),
            }
        }
    }
}
impl datapath_recovery_info_t {
    #[inline]
    pub fn new_ofed(index: usize, mempool: *mut ::std::os::raw::c_void) -> Self {
        datapath_recovery_info_t {
            ofed_recovery_info: ofed_recovery_info_t { index, mempool },
        }
    }
}

/// Metadata object (read only reference to datapath allocated buffer).
#[repr(C)]
pub struct datapath_metadata_t {
    /// Actual data buffer
    pub buffer: *mut c_void,
    /// Application offset
    pub offset: usize,
    /// Application data length
    pub len: usize,
    /// Recovery information
    pub recovery_info: datapath_recovery_info_t,
    /// (For receiving packets: sockaddr_t)
    pub metadata_addr: Option<sockaddr>,
}

impl std::fmt::Debug for datapath_metadata_t {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("datapath_metadata_t")
            .field("data_addr", &self.buffer)
            .field("offset", &self.offset)
            .field("len", &self.len)
            .field("recover_info", &self.recovery_info)
            .field("metadata_addr", &self.metadata_addr)
            .finish()
    }
}

impl AsRef<[u8]> for datapath_metadata_t {
    fn as_ref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts((self.buffer as *mut u8).offset(self.offset as isize), self.len) }
    }
}

impl Drop for datapath_metadata_t {
    fn drop(&mut self) {
        if self.buffer.is_null() {
            return;
        }
        unsafe {
            match self.recovery_info {
                datapath_recovery_info_t { ofed_recovery_info } => {
                    // magically access the libmlx5 bindings
                    #[cfg(feature = "libmlx5")]
                    {
                        crate::runtime::libmlx5::mlx5_bindings::custom_mlx5_refcnt_update_or_free(
                            ofed_recovery_info.mempool as _,
                            self.buffer,
                            ofed_recovery_info.index as _,
                            -1i8,
                        );
                    }
                    #[cfg(not(feature = "libmlx5"))]
                    {
                        unimplemented!();
                    }
                },
            }
        }
    }
}

impl Clone for datapath_metadata_t {
    fn clone(&self) -> Self {
        if !(self.buffer.is_null()) {
            unsafe {
                match self.recovery_info {
                    datapath_recovery_info_t {
                        ofed_recovery_info: ofed_info,
                    } => {
                        // magically access the libmlx5 bindings
                        #[cfg(feature = "libmlx5")]
                        {
                            crate::runtime::libmlx5::mlx5_bindings::custom_mlx5_refcnt_update_or_free(
                                ofed_info.mempool as _,
                                self.buffer,
                                ofed_info.index as _,
                                1i8,
                            );
                        }
                        #[cfg(not(feature = "libmlx5"))]
                        {
                            unimplemented!();
                        }
                    },
                }
            }
        }
        datapath_metadata_t {
            buffer: self.buffer,
            offset: self.offset,
            len: self.len,
            recovery_info: self.recovery_info.clone(),
            metadata_addr: self.metadata_addr.clone(),
        }
    }
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
                    mempool: std::ptr::null_mut(),
                },
            },
            metadata_addr: None,
        }
    }

    pub fn data_len(&self) -> usize {
        self.len
    }

    pub fn offset(&self) -> usize {
        self.offset
    }

    pub fn set_data_len_and_offset(&mut self, data_len: usize, offset: usize) -> Result<(), Fail> {
        if offset < self.offset && data_len > self.len {
            return Err(Fail::new(
                libc::EINVAL,
                "Cannot set data len and offset with offset < self.offset && data_len > self.len",
            ));
        }
        self.len = data_len;
        self.offset = offset;
        Ok(())
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

impl Drop for datapath_buffer_t {
    fn drop(&mut self) {
        // decrement reference count for buffer by 1
        #[cfg(feature = "libmlx5")]
        {
            unsafe {
                match self.recovery_info {
                    datapath_recovery_info_t {
                        ofed_recovery_info: ofed_info,
                    } => {
                        crate::runtime::libmlx5::mlx5_bindings::custom_mlx5_refcnt_update_or_free(
                            ofed_info.mempool as _,
                            self.buffer,
                            ofed_info.index as _,
                            -1i8,
                        );
                    },
                }
            }
        }
    }
}

impl datapath_buffer_t {
    pub fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let bytes_to_write = bytes.len();
        let buf_addr = (self.buffer as usize + self.data_len) as *mut u8;
        let mut buf = unsafe { std::slice::from_raw_parts_mut(buf_addr, bytes_to_write) };
        self.data_len += bytes_to_write;
        buf.write(&bytes[0..bytes_to_write])?;
        Ok(bytes.len())
    }

    pub fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }

    pub fn as_ref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.buffer as *mut u8, self.data_len) }
    }

    pub fn to_metadata(&self, off: usize, len: usize) -> datapath_metadata_t {
        // should increment the reference count by 1
        #[cfg(feature = "libmlx5")]
        {
            unsafe {
                match self.recovery_info {
                    datapath_recovery_info_t {
                        ofed_recovery_info: ofed_info,
                    } => {
                        crate::runtime::libmlx5::mlx5_bindings::custom_mlx5_refcnt_update_or_free(
                            ofed_info.mempool as _,
                            self.buffer,
                            ofed_info.index as _,
                            1i8,
                        );
                    },
                }
            }
        }
        datapath_metadata_t {
            buffer: self.buffer,
            offset: off,
            len,
            recovery_info: self.recovery_info,
            metadata_addr: None,
        }
    }
}
