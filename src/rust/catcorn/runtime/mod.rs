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
        custom_mlx5_alloc_global_context,
        custom_mlx5_get_global_context_size,
        custom_mlx5_get_per_thread_context,
        custom_mlx5_get_per_thread_context_size,
        custom_mlx5_get_registered_mempool_size,
        custom_mlx5_init_ibv_context,
        custom_mlx5_init_rx_mempools,
        custom_mlx5_init_rxq,
        custom_mlx5_init_txq,
        custom_mlx5_pci_addr,
        custom_mlx5_pci_str_to_addr,
        custom_mlx5_per_thread_context,
        custom_mlx5_qs_init_flows,
        custom_mlx5_set_rx_mempool_ptr,
        custom_mlx5_teardown,
        eth_addr,
        ibv_access_flags_IBV_ACCESS_LOCAL_WRITE,
        mlx5_rte_memcpy,
    },
    network::{
        config::{
            ArpConfig,
            TcpConfig,
            UdpConfig,
        },
        types::MacAddress,
    },
    types::{
        datapath_buffer_t,
        datapath_metadata_t,
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

//==============================================================================
// Structures
//==============================================================================
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
        let memory_manager = MemoryManager::new(&global_context_rc, 1, rx_mempool_ptrs[0], &tx_mempool_params)?;

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
            link_addr: mac_address,
            ipv4_addr,
            arp_options,
            tcp_options,
            udp_options,
        })
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
