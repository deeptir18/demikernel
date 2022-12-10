// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

//==============================================================================
// Imports
//==============================================================================

use super::Mlx5Runtime;
use crate::{
    inetstack::protocols::ethernet2::MIN_PAYLOAD_SIZE,
    runtime::{
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
    fn transmit(&self, buf: Box<dyn PacketBuf>) {
        // 1: allocate a tx mbuf for potentially the packet header and the object header
        let header_buf_option = match self.mm.alloc_tx_buffer() {
            Ok(buf_option) => buf_option,
            Err(e) => panic!("Failed to allocate header mbuf: {:?}", e.cause),
        };
        let (mut header_buf, max_len) = match header_buf_option {
            Some((buf, max_len)) => (buf, max_len),
            None => {
                panic!("Failed to allocate header mbuf; returned None.");
            },
        };

        // write the header into the given buffer
        let header_size = buf.header_size();
        assert!(header_size <= max_len);
        buf.write_header(header_buf.mut_slice(0, header_size).unwrap());
        header_buf.incr_len(header_size);

        if let Some(inner_buf) = buf.take_body() {
            match inner_buf {
                Buffer::Heap(_dbuf) => {
                    warn!("Transmit buffer is heap allocated");
                    unimplemented!();
                },
                Buffer::CornflakesObj(_obj_enum) => {
                    warn!("Trying to send cornflakes obj - not implemented yet");
                },
                Buffer::MetadataObj(data_buf) => {
                    self.transmit_header_and_data_segment(header_buf.to_metadata(0, header_size), data_buf);
                },
            }
        } else {
            // no body, just header
            if header_size < MIN_PAYLOAD_SIZE {
                let padding_bytes = MIN_PAYLOAD_SIZE - header_size;
                let padding_buf = header_buf.mut_slice(header_size, padding_bytes).unwrap();
                for byte in padding_buf {
                    *byte = 0;
                }
                header_buf.incr_len(padding_bytes);
            }

            // turn into metadata and post single metadata
            let metadata = header_buf.to_metadata(0, header_size);
            self.transmit_header_only_segment(metadata);
        }
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
