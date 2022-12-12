// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.
mod network;
use super::{
    check,
    memory::{
        sizes::{
            MempoolAllocationParams,
            RX_MEMPOOL_DATA_LEN,
            RX_MEMPOOL_DATA_PGSIZE,
            RX_MEMPOOL_MIN_NUM_ITEMS,
            TX_MEMPOOL_DATA_LEN,
            TX_MEMPOOL_DATA_PGSIZE,
            TX_MEMPOOL_MIN_NUM_ITEMS,
        },
        MemoryManager,
    },
};
use crate::runtime::{
    fail::Fail,
    libmlx5::mlx5_bindings::{
        custom_mlx5_add_completion_info,
        custom_mlx5_add_dpseg,
        custom_mlx5_alloc_global_context,
        custom_mlx5_completion_start,
        custom_mlx5_dpseg_start,
        custom_mlx5_fill_in_hdr_segment,
        custom_mlx5_finish_single_transmission,
        custom_mlx5_get_global_context_size,
        custom_mlx5_get_per_thread_context,
        custom_mlx5_get_per_thread_context_size,
        custom_mlx5_get_registered_mempool_size,
        custom_mlx5_init_ibv_context,
        custom_mlx5_init_rx_mempools,
        custom_mlx5_init_rxq,
        custom_mlx5_init_txq,
        custom_mlx5_num_octowords,
        custom_mlx5_num_wqes_available,
        custom_mlx5_num_wqes_required,
        custom_mlx5_pci_addr,
        custom_mlx5_pci_str_to_addr,
        custom_mlx5_per_thread_context,
        custom_mlx5_post_transmissions,
        custom_mlx5_process_completions,
        custom_mlx5_qs_init_flows,
        custom_mlx5_refcnt_update_or_free,
        custom_mlx5_set_rx_mempool_ptr,
        custom_mlx5_teardown,
        custom_mlx5_transmission_info,
        eth_addr,
        ibv_access_flags_IBV_ACCESS_LOCAL_WRITE,
        mlx5_rte_memcpy,
        mlx5_wqe_ctrl_seg,
        mlx5_wqe_data_seg,
        recv_mbuf_info,
        registered_mempool,
        MLX5_ETH_WQE_L3_CSUM,
        MLX5_ETH_WQE_L4_CSUM,
    },
    memory::CornflakesObj,
    network::{
        config::{
            ArpConfig,
            TcpConfig,
            UdpConfig,
        },
        consts::RECEIVE_BATCH_SIZE,
        types::MacAddress,
    },
    types::{
        datapath_buffer_t,
        datapath_metadata_t,
        datapath_recovery_info_t,
    },
    Runtime,
};
use std::{
    boxed::Box,
    collections::HashMap,
    ffi::CString,
    mem::MaybeUninit,
    net::Ipv4Addr,
    rc::Rc,
    time::Duration,
};

const COMPLETION_BUDGET: usize = 32;

//==============================================================================
// Structures
//==============================================================================
/// RecvMbufArray
#[derive(Debug, PartialEq, Eq)]
pub struct RecvMbufArray {
    array_ptr: *mut [u8],
}

impl RecvMbufArray {
    pub fn new(size: usize) -> RecvMbufArray {
        let array_ptr_box = vec![0u8; size * std::mem::size_of::<recv_mbuf_info>()].into_boxed_slice();
        let array_ptr = Box::<[u8]>::into_raw(array_ptr_box);
        for i in 0..size {
            let ptr = unsafe {
                (array_ptr as *mut u8).offset((i * std::mem::size_of::<recv_mbuf_info>()) as isize)
                    as *mut recv_mbuf_info
            };
            unsafe {
                (*ptr).buf_addr = std::ptr::null_mut();
                (*ptr).mempool = std::ptr::null_mut();
                (*ptr).ref_count_index = 0;
                (*ptr).rss_hash = 0;
            }
        }
        RecvMbufArray { array_ptr }
    }

    pub fn as_recv_mbuf_info_array_ptr(&self) -> *mut recv_mbuf_info {
        self.array_ptr as *mut recv_mbuf_info
    }

    pub fn get(&self, i: usize) -> *mut recv_mbuf_info {
        unsafe {
            (self.array_ptr as *mut u8).offset((i * std::mem::size_of::<recv_mbuf_info>()) as isize)
                as *mut recv_mbuf_info
        }
    }
}

impl Drop for RecvMbufArray {
    fn drop(&mut self) {
        unsafe {
            let _ = Box::from_raw(self.array_ptr);
        }
    }
}

/// Mlx5GlobalContext
pub struct Mlx5GlobalContext {
    num_threads: usize,
    global_context_ptr: *mut [u8],
    thread_context_ptr: *mut [u8],
}

/// Mlx5PerThreadContext
#[derive(Clone)]
pub struct Mlx5Runtime {
    mlx5_global_context: Rc<Mlx5GlobalContext>,
    queue_id: u16,
    mm: MemoryManager,
    recv_mbuf_array: Rc<RecvMbufArray>,
    pub link_addr: MacAddress,
    pub ipv4_addr: Ipv4Addr,
    pub arp_options: ArpConfig,
    pub tcp_options: TcpConfig,
    pub udp_options: UdpConfig,
}

//==============================================================================
// Associate Functions
//==============================================================================

impl Mlx5GlobalContext {
    pub fn new(
        num_threads: usize,
        mac_address: MacAddress,
        pci_address: String,
    ) -> Result<(Self, Vec<*mut [u8]>), Fail> {
        // TODO: how do threads work in demikernel?
        // create a box to hold global context and per-thread contexts
        let global_context_size = unsafe { custom_mlx5_get_global_context_size() };
        let thread_context_size = unsafe { custom_mlx5_get_per_thread_context_size(1) };
        let global_context_box: Box<[u8]> = vec![0u8; global_context_size as _].into_boxed_slice();
        let thread_context_box: Box<[u8]> = vec![0u8; thread_context_size as _].into_boxed_slice();
        let global_context_ptr = Box::<[u8]>::into_raw(global_context_box);
        let thread_context_ptr = Box::<[u8]>::into_raw(thread_context_box);

        unsafe { custom_mlx5_alloc_global_context(num_threads as _, global_context_ptr as _, thread_context_ptr as _) };
        let mut rx_mempool_ptrs: Vec<*mut [u8]> = Vec::with_capacity(num_threads as _);
        // initialize a recieve mempool for each thread
        for i in 0..num_threads {
            let rx_mempool_box: Box<[u8]> =
                vec![0; unsafe { custom_mlx5_get_registered_mempool_size() as _ }].into_boxed_slice();
            let rx_mempool_ptr = Box::<[u8]>::into_raw(rx_mempool_box);
            debug!("Allocated rx mempool ptr at {:?}", rx_mempool_ptr);
            unsafe { custom_mlx5_set_rx_mempool_ptr(global_context_ptr as _, i as _, rx_mempool_ptr as _) };
            rx_mempool_ptrs.push(rx_mempool_ptr);
        }

        // initialize ibv context
        // get pci addr type to pass in
        let pci_str = CString::new(pci_address.as_str()).unwrap();
        let mut custom_mlx5_pci_addr_c: MaybeUninit<custom_mlx5_pci_addr> = MaybeUninit::zeroed();
        unsafe {
            custom_mlx5_pci_str_to_addr(pci_str.as_ptr() as _, custom_mlx5_pci_addr_c.as_mut_ptr() as _);
        }
        unsafe {
            check(
                "custom_mlx5_init_ibv_context",
                custom_mlx5_init_ibv_context(global_context_ptr as _, custom_mlx5_pci_addr_c.as_mut_ptr()),
            )?
        };

        // initialize and register the rx mempools
        let rx_mempool_params: MempoolAllocationParams =
            MempoolAllocationParams::new(RX_MEMPOOL_MIN_NUM_ITEMS, RX_MEMPOOL_DATA_PGSIZE, RX_MEMPOOL_DATA_LEN)?;
        unsafe {
            check(
                "custom_mlx5_init_rx_mempools",
                custom_mlx5_init_rx_mempools(
                    global_context_ptr as _,
                    rx_mempool_params.get_item_len() as _,
                    rx_mempool_params.get_num_items() as _,
                    rx_mempool_params.get_data_pgsize() as _,
                    ibv_access_flags_IBV_ACCESS_LOCAL_WRITE as _,
                ),
            )?;
        }

        // init queues
        for i in 0..num_threads {
            let per_thread_context = unsafe { custom_mlx5_get_per_thread_context(global_context_ptr as _, i as u64) };
            unsafe {
                check("custom_mlx5_init_rxq", custom_mlx5_init_rxq(per_thread_context))?;
                check("custom_mlx5_txq", custom_mlx5_init_txq(per_thread_context))?;
            }
        }

        // init queue steering
        let mut ether_addr: MaybeUninit<eth_addr> = MaybeUninit::zeroed();
        unsafe {
            mlx5_rte_memcpy(ether_addr.as_mut_ptr() as _, mac_address.as_bytes().as_ptr() as _, 6);
            check(
                "custom_mlx5_qs_init_flows",
                custom_mlx5_qs_init_flows(global_context_ptr as _, ether_addr.as_mut_ptr()),
            )?;
        }

        Ok((
            Mlx5GlobalContext {
                num_threads,
                global_context_ptr,
                thread_context_ptr,
            },
            rx_mempool_ptrs,
        ))
    }

    pub fn get_thread_context_ptr(&self, thread_id: usize) -> *mut custom_mlx5_per_thread_context {
        unsafe { custom_mlx5_get_per_thread_context(self.global_context_ptr as _, thread_id as u64) }
    }
}

impl Drop for Mlx5GlobalContext {
    fn drop(&mut self) {
        // for each thread, do thread teardown
        for i in 0..self.num_threads {
            unsafe {
                custom_mlx5_teardown(self.get_thread_context_ptr(i));
            }
        }
        // free thread and global context box
        unsafe {
            let _ = Box::from_raw(self.global_context_ptr);
            let _ = Box::from_raw(self.thread_context_ptr);
        }
    }
}

/// Associate Functions for DPDK Runtime
impl Mlx5Runtime {
    pub fn new(
        num_queues: usize,
        ipv4_addr: Ipv4Addr,
        mac_address: MacAddress,
        pci_address: String,
        arp_table: HashMap<Ipv4Addr, MacAddress>,
        disable_arp: bool,
        _use_jumbo_frames: bool,
        _mtu: u16,
        mss: usize,
        tcp_checksum_offload: bool,
        udp_checksum_offload: bool,
    ) -> Result<Mlx5Runtime, Fail> {
        if num_queues > 1 {
            return Err(Fail::new(libc::EINVAL, "Mlx5 does not support more than 1 queue."));
        }
        let (mlx5_global_context, rx_mempool_ptrs) = Mlx5GlobalContext::new(num_queues, mac_address, pci_address)?;
        let tx_mempool_params: MempoolAllocationParams =
            MempoolAllocationParams::new(TX_MEMPOOL_MIN_NUM_ITEMS, TX_MEMPOOL_DATA_PGSIZE, TX_MEMPOOL_DATA_LEN)?;
        let global_context_rc = Rc::new(mlx5_global_context);
        let memory_manager = MemoryManager::new(&global_context_rc, 0, rx_mempool_ptrs[0], &tx_mempool_params)?;

        let arp_options = ArpConfig::new(
            Some(Duration::from_secs(15)),
            Some(Duration::from_secs(20)),
            Some(5),
            Some(arp_table),
            Some(disable_arp),
        );

        let tcp_options = TcpConfig::new(
            Some(mss),
            None,
            None,
            Some(0xffff),
            Some(0),
            None,
            Some(tcp_checksum_offload),
            Some(tcp_checksum_offload),
        );

        let udp_options = UdpConfig::new(Some(udp_checksum_offload), Some(udp_checksum_offload));

        Ok(Self {
            mlx5_global_context: global_context_rc,
            queue_id: 0u16,
            mm: memory_manager,
            recv_mbuf_array: Rc::new(RecvMbufArray::new(RECEIVE_BATCH_SIZE)),
            link_addr: mac_address,
            ipv4_addr,
            arp_options,
            tcp_options,
            udp_options,
        })
    }

    /// For a particular number of segments and inline length, return wqes required
    fn wqes_required(&self, inline_len: usize, num_segs: usize) -> (usize, usize) {
        let num_octowords = unsafe { custom_mlx5_num_octowords(inline_len as _, num_segs as _) };
        let num_wqes = unsafe { custom_mlx5_num_wqes_required(num_octowords as _) };
        (num_octowords as _, num_wqes as _)
    }

    /// Fill in the header
    fn start_dma_request(
        &self,
        num_octowords: usize,
        num_wqes: usize,
        inline_len: usize,
        num_segs: usize,
        flags: i32,
    ) -> *mut mlx5_wqe_ctrl_seg {
        unsafe {
            custom_mlx5_fill_in_hdr_segment(
                self.mlx5_global_context.get_thread_context_ptr(self.queue_id as _),
                num_octowords as _,
                num_wqes as _,
                inline_len as _,
                num_segs as _,
                flags as _,
            )
        }
    }

    /// Spins on waiting for available wqes.
    fn spin_on_available_wqes(&self, num_wqes_needed: usize) {
        let mut curr_available_wqes: usize = unsafe {
            custom_mlx5_num_wqes_available(self.mlx5_global_context.get_thread_context_ptr(self.queue_id as _)) as usize
        };
        while num_wqes_needed > curr_available_wqes {
            // because we don't support batching yet, just poll for completions
            self.poll_for_completions();
            curr_available_wqes = unsafe {
                custom_mlx5_num_wqes_available(self.mlx5_global_context.get_thread_context_ptr(self.queue_id as _))
            } as usize;
        }
        return;
    }

    fn transmit_header_and_cornflakes_obj(&self, mut header_buffer: datapath_buffer_t, cornflakes_obj: CornflakesObj) {
        debug!("Reached cornflakes function");
        // wait till number of segments are available
        let inline_len = 0;
        let num_segs_required = cornflakes_obj.num_segments_total(true);
        debug!("Num segs required: {}", num_segs_required);
        let (num_octowords, num_wqes) = self.wqes_required(inline_len, num_segs_required);
        debug!("Num wqes: {}, num_octowords {}", num_wqes, num_octowords);
        self.spin_on_available_wqes(num_wqes);

        // write header into header buffer
        // write pkt timestamp and flow id
        // TODO: should handle case where pkt timestamp and flow id don't even fit
        if cornflakes_obj.offset() < 32 {
            if cornflakes_obj.offset() == 0 {
                debug!("Writing two u64s at the front");
                header_buffer.write_u64(cornflakes_obj.get_timestamp());
                header_buffer.write_u64(0);
                header_buffer.write_u64(cornflakes_obj.get_flow_id());
                header_buffer.write_u64(0);
            } else {
                // handle case of writing partial timestamp and flow id
                todo!();
            }
        }
        debug!(
            "Header buf len: {} (before writing cornflakes data)",
            header_buffer.len()
        );
        let mut_header_slice = header_buffer
            .mut_slice(header_buffer.len(), header_buffer.max_len() - header_buffer.len())
            .unwrap();
        let written = cornflakes_obj.write_header(mut_header_slice);
        header_buffer.incr_len(written);
        let header_segment = header_buffer.to_metadata(0, header_buffer.len());
        debug!(
            "Header buf len: {} (after writing cornflakes data",
            header_segment.data_len()
        );

        // start transmission
        let ctrl_seg = self.start_dma_request(
            num_octowords,
            num_wqes,
            inline_len,
            num_segs_required,
            MLX5_ETH_WQE_L3_CSUM as i32 | MLX5_ETH_WQE_L4_CSUM as i32,
        );

        let mut dpseg =
            unsafe { custom_mlx5_dpseg_start(self.mlx5_global_context.get_thread_context_ptr(self.queue_id as _), 0) };
        let mut completion = unsafe {
            custom_mlx5_completion_start(self.mlx5_global_context.get_thread_context_ptr(self.queue_id as _))
        };
        let (curr_dpseg, curr_completion) = self.post_pcie_request(header_segment, dpseg, completion);
        dpseg = curr_dpseg;
        completion = curr_completion;

        // define a callback to post on the ring buffer, and call it with the object iterator
        let mut ring_buffer_state = (dpseg, completion);
        let thread_context_ptr = self.mlx5_global_context.get_thread_context_ptr(self.queue_id as _);
        let mut callback = |metadata: datapath_metadata_t,
                            ring_buffer_state: &mut (*mut mlx5_wqe_data_seg, *mut custom_mlx5_transmission_info)|
         -> Result<(), Fail> {
            debug!("In callback");
            // increment reference count on underlying metadata
            unsafe {
                match metadata.recovery_info {
                    datapath_recovery_info_t { ofed_recovery_info } => {
                        custom_mlx5_refcnt_update_or_free(
                            ofed_recovery_info.mempool as _,
                            metadata.buffer,
                            ofed_recovery_info.index as _,
                            -1i8,
                        );
                        debug!(
                            "{}",
                            format!(
                                "Posting buffer: len = {}, off = {}, buf addr = {:?}",
                                metadata.data_len(),
                                metadata.offset(),
                                metadata.buffer
                            )
                        );
                        let curr_dpseg = custom_mlx5_add_dpseg(
                            thread_context_ptr,
                            ring_buffer_state.0,
                            metadata.buffer,
                            ofed_recovery_info.mempool as *mut registered_mempool,
                            metadata.offset() as _,
                            metadata.data_len() as _,
                        );
                        let curr_completion = custom_mlx5_add_completion_info(
                            thread_context_ptr,
                            ring_buffer_state.1,
                            metadata.buffer,
                            ofed_recovery_info.mempool as *mut registered_mempool,
                        );
                        ring_buffer_state.0 = curr_dpseg;
                        ring_buffer_state.1 = curr_completion;
                    },
                }
            }
            Ok(())
        };

        cornflakes_obj.iterate_over_entries_with_callback(&mut callback, &mut ring_buffer_state);

        // finish transmission and poll for completions
        self.finish_dma_request(num_wqes);
        self.ring_doorbell(ctrl_seg);
        self.poll_for_completions();
    }

    fn transmit_header_and_data_segment(&self, header_segment: datapath_metadata_t, data_segment: datapath_metadata_t) {
        let inline_len = 0;
        let num_segs = 2;
        let (num_octowords, num_wqes) = self.wqes_required(inline_len, num_segs);
        self.spin_on_available_wqes(num_wqes);
        let ctrl_seg = self.start_dma_request(
            num_octowords,
            num_wqes,
            inline_len,
            num_segs,
            MLX5_ETH_WQE_L3_CSUM as i32 | MLX5_ETH_WQE_L4_CSUM as i32,
        );
        let mut dpseg =
            unsafe { custom_mlx5_dpseg_start(self.mlx5_global_context.get_thread_context_ptr(self.queue_id as _), 0) };
        let mut completion = unsafe {
            custom_mlx5_completion_start(self.mlx5_global_context.get_thread_context_ptr(self.queue_id as _))
        };
        let (curr_dpseg, curr_completion) = self.post_pcie_request(header_segment, dpseg, completion);
        dpseg = curr_dpseg;
        completion = curr_completion;
        let _ = self.post_pcie_request(data_segment, dpseg, completion);
        self.finish_dma_request(num_wqes);
        self.ring_doorbell(ctrl_seg);
        self.poll_for_completions();
    }

    /// Sends a "single metadata" request (header segment only).
    fn transmit_header_only_segment(&self, header_segment: datapath_metadata_t) {
        debug!("Transmit header only segment");
        let inline_len = 0;
        let num_segs = 1;
        let (num_octowords, num_wqes) = self.wqes_required(inline_len, num_segs);
        self.spin_on_available_wqes(num_wqes);
        debug!("Finished spinning");
        let ctrl_seg = self.start_dma_request(
            num_octowords,
            num_wqes,
            inline_len,
            num_segs,
            MLX5_ETH_WQE_L3_CSUM as i32 | MLX5_ETH_WQE_L4_CSUM as i32,
        );
        let dpseg_start =
            unsafe { custom_mlx5_dpseg_start(self.mlx5_global_context.get_thread_context_ptr(self.queue_id as _), 0) };
        let completion_start = unsafe {
            custom_mlx5_completion_start(self.mlx5_global_context.get_thread_context_ptr(self.queue_id as _))
        };
        let _ = self.post_pcie_request(header_segment, dpseg_start, completion_start);
        self.finish_dma_request(num_wqes);
        self.ring_doorbell(ctrl_seg);
        self.poll_for_completions();
        debug!("done with transmit");
    }

    /// Sends the given metadata (and rings doorbell).
    /// Increments the reference count on the metadata before sending.
    /// Polls for completions before returning.
    fn post_pcie_request(
        &self,
        metadata: datapath_metadata_t,
        curr_dpseg: *mut mlx5_wqe_data_seg,
        curr_completion: *mut custom_mlx5_transmission_info,
    ) -> (*mut mlx5_wqe_data_seg, *mut custom_mlx5_transmission_info) {
        // increment the reference count on the metadata (for NIC access)
        unsafe {
            match metadata.recovery_info {
                datapath_recovery_info_t { ofed_recovery_info } => {
                    custom_mlx5_refcnt_update_or_free(
                        ofed_recovery_info.mempool as _,
                        metadata.buffer,
                        ofed_recovery_info.index as _,
                        -1i8,
                    );
                    debug!(
                        "{}",
                        format!(
                            "Posting buffer: len = {}, off = {}, buf addr = {:?}",
                            metadata.data_len(),
                            metadata.offset(),
                            metadata.buffer
                        )
                    );
                    (
                        custom_mlx5_add_dpseg(
                            self.mlx5_global_context.get_thread_context_ptr(self.queue_id as _),
                            curr_dpseg,
                            metadata.buffer,
                            ofed_recovery_info.mempool as *mut registered_mempool,
                            metadata.offset() as _,
                            metadata.data_len() as _,
                        ),
                        custom_mlx5_add_completion_info(
                            self.mlx5_global_context.get_thread_context_ptr(self.queue_id as _),
                            curr_completion,
                            metadata.buffer,
                            ofed_recovery_info.mempool as *mut registered_mempool,
                        ),
                    )
                },
            }
        }
    }

    /// "Finishes" a transmission onto the ring buffer by updating local ring buffer state.
    fn finish_dma_request(&self, num_wqes_used: usize) {
        unsafe {
            custom_mlx5_finish_single_transmission(
                self.mlx5_global_context.get_thread_context_ptr(self.queue_id as _),
                num_wqes_used as _,
            );
        }
    }

    fn ring_doorbell(&self, ctrl_seg: *mut mlx5_wqe_ctrl_seg) {
        if unsafe {
            custom_mlx5_post_transmissions(
                self.mlx5_global_context.get_thread_context_ptr(self.queue_id as _),
                ctrl_seg,
            ) != 0
        } {
            panic!("Failed to ring doorbell.");
        }
    }

    fn poll_for_completions(&self) {
        if unsafe {
            custom_mlx5_process_completions(
                self.mlx5_global_context.get_thread_context_ptr(self.queue_id as _),
                COMPLETION_BUDGET as _,
            )
        } != 0
        {
            panic!("Failed to process completions.");
        }
    }

    pub fn recover_metadata(&self, ptr: &[u8]) -> Result<Option<datapath_metadata_t>, Fail> {
        self.mm.recover_metadata(ptr)
    }

    pub fn allocate_buffer(&self, size: usize) -> Result<Option<datapath_buffer_t>, Fail> {
        self.mm.alloc_buffer(size)
    }

    pub fn allocate_tx_buffer(&self) -> Result<Option<(datapath_buffer_t, usize)>, Fail> {
        self.mm.alloc_tx_buffer()
    }
}

//==============================================================================
// Trait Implementations
//==============================================================================

impl Runtime for Mlx5Runtime {}
