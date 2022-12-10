// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

//==============================================================================
// Imports
//==============================================================================

use super::Mlx5Runtime;
use crate::runtime::{
    libmlx5::mlx5_bindings::custom_mlx5_gather_rx,
    memory::Buffer,
    network::{
        consts::RECEIVE_BATCH_SIZE,
        NetworkRuntime,
        PacketBuf,
    },
    types::{
        datapath_metadata_t,
        datapath_recovery_info_t,
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
        let mut out = ArrayVec::new();
        let received = unsafe {
            #[cfg(feature = "profiler")]
            timer!("catcorn_libos::receive::custom_mlx5_gather_rx");
            custom_mlx5_gather_rx(
                self.mlx5_global_context.get_thread_context_ptr(self.queue_id as _),
                self.recv_mbuf_array.as_recv_mbuf_info_array_ptr(),
                RECEIVE_BATCH_SIZE as _,
            )
        };
        assert!(received as usize <= RECEIVE_BATCH_SIZE);
        {
            #[cfg(feature = "profiler")]
            timer!("catcorn_libos:receive::for");
            for i in 0..received {
                let recv_mbuf_info = self.recv_mbuf_array.get(i as usize);
                let buffer_addr = unsafe { access!(recv_mbuf_info, buf_addr) };
                let mempool = unsafe { access!(recv_mbuf_info, mempool) };
                let index = unsafe { access!(recv_mbuf_info, ref_count_index) };
                let pkt_len = unsafe { access!(recv_mbuf_info, pkt_len) };
                let datapath_metadata = datapath_metadata_t {
                    buffer: buffer_addr,
                    offset: 0,
                    len: pkt_len as usize,
                    recovery_info: datapath_recovery_info_t::new_ofed(index as usize, mempool as _),
                    metadata_addr: None,
                };
                let buf: Buffer = Buffer::MetadataObj(datapath_metadata);
                out.push(buf);
                unsafe {
                    (*recv_mbuf_info).buf_addr = std::ptr::null_mut();
                    (*recv_mbuf_info).mempool = std::ptr::null_mut();
                    (*recv_mbuf_info).ref_count_index = 0;
                    (*recv_mbuf_info).rss_hash = 0;
                }
            }
            out
        }
    }
}
