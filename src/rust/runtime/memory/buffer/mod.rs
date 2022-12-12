// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#[cfg(feature = "libmlx5")]
mod cornflakes_buffer;
mod databuffer;
#[cfg(feature = "libdpdk")]
mod dpdkbuffer;

//==============================================================================
// Imports
//==============================================================================
use core::ops::{
    Deref,
    DerefMut,
};
use std::fmt::Debug;

//==============================================================================
// Exports
//==============================================================================

#[cfg(feature = "libmlx5")]
use crate::runtime::types::datapath_metadata_t;

pub use self::databuffer::DataBuffer;
#[cfg(feature = "libdpdk")]
pub use self::dpdkbuffer::DPDKBuffer;

#[cfg(feature = "libmlx5")]
pub use self::cornflakes_buffer::CornflakesObj;
//==============================================================================
// Enumerations
//==============================================================================
#[derive(Clone, Debug)]
pub enum Buffer {
    Heap(DataBuffer),
    #[cfg(feature = "libdpdk")]
    DPDK(DPDKBuffer),
    #[cfg(feature = "libmlx5")]
    CornflakesObj(CornflakesObj),
    #[cfg(feature = "libmlx5")]
    MetadataObj(datapath_metadata_t),
}

//==============================================================================
// Associated Functions
//==============================================================================

impl Buffer {
    /// Removes bytes from the front of the target data buffer.
    pub fn adjust(&mut self, nbytes: usize) {
        match self {
            Buffer::Heap(dbuf) => dbuf.adjust(nbytes),
            #[cfg(feature = "libdpdk")]
            Buffer::DPDK(mbuf) => mbuf.adjust(nbytes),
            #[cfg(feature = "libmlx5")]
            Buffer::CornflakesObj(cornflakes_obj) => {
                cornflakes_obj.trim(nbytes);
            },
            #[cfg(feature = "libmlx5")]
            Buffer::MetadataObj(metadata) => {
                let cur_len = metadata.data_len();
                let cur_offset = metadata.offset();
                metadata
                    .set_data_len_and_offset(cur_len - nbytes, cur_offset + nbytes)
                    .unwrap();
            },
        }
    }

    /// Removes bytes from the end of the target data buffer.
    pub fn trim(&mut self, nbytes: usize) {
        match self {
            Buffer::Heap(dbuf) => dbuf.trim(nbytes),
            #[cfg(feature = "libdpdk")]
            Buffer::DPDK(mbuf) => mbuf.trim(nbytes),
            #[cfg(feature = "libmlx5")]
            Buffer::CornflakesObj(cornflakes_obj) => {
                cornflakes_obj.adjust(nbytes);
            },
            #[cfg(feature = "libmlx5")]
            Buffer::MetadataObj(metadata) => {
                let cur_len = metadata.data_len();
                let cur_offset = metadata.offset();
                metadata.set_data_len_and_offset(cur_len - nbytes, cur_offset).unwrap();
            },
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Buffer::Heap(dbuf) => dbuf.len(),
            #[cfg(feature = "libdpdk")]
            Buffer::DPDK(mbuf) => mbuf.len(),
            #[cfg(feature = "libmlx5")]
            Buffer::CornflakesObj(cornflakes_obj) => cornflakes_obj.len(),
            #[cfg(feature = "libmlx5")]
            Buffer::MetadataObj(metadata) => metadata.data_len(),
        }
    }
}

//==============================================================================
// Standard-Library Trait Implementations
//==============================================================================

/// De-Reference Trait Implementation for Data Buffers
impl Deref for Buffer {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        match self {
            Buffer::Heap(dbuf) => dbuf.deref(),
            #[cfg(feature = "libdpdk")]
            Buffer::DPDK(mbuf) => mbuf.deref(),
            #[cfg(feature = "libmlx5")]
            Buffer::CornflakesObj(_cornflakes_obj) => {
                debug!("Reaching here");
                // TODO: I don't believe its reasonable to expect buffer objects to implement this
                // function, because it assumes that scatter-gather arrays will be split as
                // separate packets, which will add a bunch of overhead.
                unimplemented!();
            },
            #[cfg(feature = "libmlx5")]
            Buffer::MetadataObj(metadata) => metadata.as_ref(),
        }
    }
}

/// Mutable De-Reference Trait Implementation for Data Buffers
impl DerefMut for Buffer {
    fn deref_mut(&mut self) -> &mut [u8] {
        match self {
            Buffer::Heap(dbuf) => dbuf.deref_mut(),
            #[cfg(feature = "libdpdk")]
            Buffer::DPDK(mbuf) => mbuf.deref_mut(),
            #[cfg(feature = "libmlx5")]
            Buffer::CornflakesObj(_cornflakes_obj) => {
                unimplemented!();
            },
            #[cfg(feature = "libmlx5")]
            Buffer::MetadataObj(_metadata) => {
                unimplemented!();
            },
        }
    }
}
