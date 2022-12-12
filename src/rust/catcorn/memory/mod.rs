// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

pub mod mem;
pub mod sizes;

// Imports
//==============================================================================
use super::{
    super::access,
    runtime::Mlx5GlobalContext,
};
use crate::runtime::{
    fail::Fail,
    libmlx5::mlx5_bindings::{
        custom_mlx5_alloc_and_register_tx_pool,
        custom_mlx5_deregister_and_free_registered_mempool,
        custom_mlx5_get_registered_mempool_size,
        custom_mlx5_mempool,
        custom_mlx5_mempool_alloc,
        custom_mlx5_mempool_find_index,
        custom_mlx5_mempool_free,
        custom_mlx5_refcnt_set,
        custom_mlx5_refcnt_update_or_free,
        get_data_mempool,
        ibv_access_flags_IBV_ACCESS_LOCAL_WRITE,
        registered_mempool,
    },
    types::{
        datapath_buffer_t,
        datapath_metadata_t,
        datapath_recovery_info_t,
        MempoolID,
    },
};
use mem::{
    closest_1g_page,
    closest_2mb_page,
    closest_4k_page,
    PGSIZE_1GB,
    PGSIZE_2MB,
    PGSIZE_4KB,
};
use sizes::MempoolAllocationParams;
use std::{
    collections::HashMap,
    rc::Rc,
};
//==============================================================================
// Structures
//==============================================================================
const RX_MEMPOOL_ID: MempoolID = 0;
const TX_MEMPOOL_ID: MempoolID = 1;
pub struct Mempool {
    mempool_ptr: *mut [u8],
    _mempool_id: MempoolID,
}

// Each thread's memory manager has a:
// tx memory pool.
// rx memory pool.
// an arbitrary amount of `user-added` memory pools.
#[derive(Clone)]
pub struct MemoryManager {
    mempools: HashMap<MempoolID, Rc<Mempool>>,
    _next_id_to_allocate: MempoolID,
    address_cache_2mb: HashMap<usize, MempoolID>,
    address_cache_4kb: HashMap<usize, MempoolID>,
    address_cache_1gb: HashMap<usize, MempoolID>,
}

//==============================================================================
// Associate Functions
//==============================================================================

impl Mempool {
    #[inline]
    pub fn new(
        mempool_params: &MempoolAllocationParams,
        queue_id: usize,
        global_context: &Rc<Mlx5GlobalContext>,
        use_atomic_ops: bool,
        _mempool_id: MempoolID,
    ) -> Result<Self, Fail> {
        let mempool_box = vec![0u8; unsafe { custom_mlx5_get_registered_mempool_size() } as _].into_boxed_slice();
        let atomic_ops: u32 = match use_atomic_ops {
            true => 1,
            false => 0,
        };
        debug!("Initializing tx mempool with queue_id {}", queue_id);
        let mempool_ptr = Box::<[u8]>::into_raw(mempool_box);
        if unsafe {
            custom_mlx5_alloc_and_register_tx_pool(
                global_context.get_thread_context_ptr(queue_id),
                mempool_ptr as _,
                mempool_params.get_item_len() as _,
                mempool_params.get_num_items() as _,
                mempool_params.get_data_pgsize() as _,
                ibv_access_flags_IBV_ACCESS_LOCAL_WRITE as _,
                atomic_ops,
            )
        } != 0
        {
            warn!("Failed to register and init mempool with params: {:?}", mempool_params);
            return Err(Fail::new(libc::EINVAL, "failed to register and init mempool"));
        }
        Ok(Mempool {
            mempool_ptr,
            _mempool_id,
        })
    }

    #[inline]
    pub fn new_from_ptr(mempool_ptr: *mut [u8], _mempool_id: MempoolID) -> Self {
        Mempool {
            mempool_ptr,
            _mempool_id,
        }
    }

    #[inline]
    fn mempool(&self) -> *mut registered_mempool {
        self.mempool_ptr as *mut registered_mempool
    }

    #[inline]
    fn data_mempool(&self) -> *mut custom_mlx5_mempool {
        unsafe { get_data_mempool(self.mempool()) }
    }

    fn get_2mb_pages(&self) -> Vec<usize> {
        let data_pool = self.data_mempool();
        let pgsize = unsafe { access!(data_pool, pgsize, usize) };
        if pgsize != PGSIZE_2MB {
            return vec![];
        }
        debug!("Returning 2mb pages");
        let num_pages = unsafe { access!(data_pool, num_pages, usize) };
        let mempool_start = unsafe { access!(data_pool, buf, usize) };
        (0..num_pages)
            .map(|i| mempool_start + pgsize * i)
            .collect::<Vec<usize>>()
    }

    fn get_4k_pages(&self) -> Vec<usize> {
        let data_pool = self.data_mempool();
        let pgsize = unsafe { access!(data_pool, pgsize, usize) };
        if pgsize != PGSIZE_4KB {
            return vec![];
        }
        let num_pages = unsafe { access!(data_pool, num_pages, usize) };
        let mempool_start = unsafe { access!(data_pool, buf, usize) };
        (0..num_pages)
            .map(|i| mempool_start + pgsize * i)
            .collect::<Vec<usize>>()
    }

    fn get_1g_pages(&self) -> Vec<usize> {
        let data_pool = self.data_mempool();
        let pgsize = unsafe { access!(data_pool, pgsize, usize) };
        if pgsize != PGSIZE_1GB {
            return vec![];
        }
        let num_pages = unsafe { access!(data_pool, num_pages, usize) };
        let mempool_start = unsafe { access!(data_pool, buf, usize) };
        (0..num_pages)
            .map(|i| mempool_start + pgsize * i)
            .collect::<Vec<usize>>()
    }

    #[inline]
    pub fn alloc_buf(&self) -> Result<Option<datapath_buffer_t>, Fail> {
        let data = unsafe { custom_mlx5_mempool_alloc(self.data_mempool()) };
        if data.is_null() {
            warn!("Allocated none from memory pool at {:?}", self.mempool());
            return Ok(None);
        }
        // recover the reference count index
        let index = unsafe { custom_mlx5_mempool_find_index(self.data_mempool(), data) };
        if index < 0 {
            unsafe {
                custom_mlx5_mempool_free(self.data_mempool(), data);
            }
            warn!("Couldn't find index; was {}", index);
            return Ok(None);
        }
        // set datapath buffer reference count as 1
        unsafe {
            custom_mlx5_refcnt_set(self.mempool(), index as _, 1u8);
        }
        Ok(Some(datapath_buffer_t {
            buffer: data,
            data_len: 0,
            max_len: unsafe { access!(self.data_mempool(), item_len, usize) },
            recovery_info: datapath_recovery_info_t::new_ofed(index as _, self.mempool() as _),
        }))
    }

    #[inline]
    pub unsafe fn recover_metadata_mbuf(&self, ptr: &[u8]) -> datapath_metadata_t {
        let mempool = self.mempool();
        let data_pool = self.data_mempool();
        let mempool_start = access!(data_pool, buf, usize);
        let item_len = access!(data_pool, item_len, usize);
        let offset_within_alloc = ptr.as_ptr() as usize - mempool_start;
        let index = (offset_within_alloc & !(item_len - 1)) >> access!(data_pool, log_item_len, usize);
        let data_ptr = mempool_start + (index << access!(data_pool, log_item_len, usize));

        // before returning the metadata, increment underlying reference count
        unsafe {
            custom_mlx5_refcnt_update_or_free(
                self.mempool() as _,
                data_ptr as *mut ::std::os::raw::c_void,
                index as _,
                1i8,
            );
        }
        datapath_metadata_t {
            buffer: data_ptr as *mut ::std::os::raw::c_void,
            offset: ptr.as_ptr() as usize - data_ptr as usize,
            len: ptr.len(),
            recovery_info: datapath_recovery_info_t::new_ofed(index, mempool as _),
            metadata_addr: None,
        }
    }
}

impl Drop for Mempool {
    fn drop(&mut self) {
        unsafe {
            // drop pages behind mempool
            if custom_mlx5_deregister_and_free_registered_mempool(self.mempool()) != 0 {
                warn!("Failed to deregister and free backing mempool at {:?}", self.mempool());
            }
            // drop allocated box for mempool
            let _ = Box::from_raw(self.mempool_ptr);
        }
    }
}

impl MemoryManager {
    pub fn new(
        global_context: &Rc<Mlx5GlobalContext>,
        queue_id: usize,
        rx_mempool_ptr: *mut [u8],
        tx_allocation_params: &sizes::MempoolAllocationParams,
    ) -> Result<Self, Fail> {
        // implicitly assign rx mempool to mempool ID 0
        let rx_mempool = Rc::new(Mempool::new_from_ptr(rx_mempool_ptr, RX_MEMPOOL_ID));
        // implicitly assign tx mempool to id 1
        let tx_mempool = Rc::new(Mempool::new(
            tx_allocation_params,
            queue_id,
            global_context,
            false,
            TX_MEMPOOL_ID,
        )?);
        // add in 2g, 4k and 1G pages for rx mempool to hashmap
        let mut address_cache_2mb: HashMap<usize, MempoolID> = HashMap::default();
        for page in rx_mempool.get_2mb_pages() {
            address_cache_2mb.insert(page, RX_MEMPOOL_ID);
        }
        let mut address_cache_4kb: HashMap<usize, MempoolID> = HashMap::default();
        for page in rx_mempool.get_4k_pages() {
            address_cache_4kb.insert(page, RX_MEMPOOL_ID);
        }
        let mut address_cache_1gb: HashMap<usize, MempoolID> = HashMap::default();
        for page in rx_mempool.get_1g_pages() {
            address_cache_1gb.insert(page, RX_MEMPOOL_ID);
        }
        let mut mempools_hashmap: HashMap<MempoolID, Rc<Mempool>> = HashMap::default();
        mempools_hashmap.insert(RX_MEMPOOL_ID, rx_mempool);
        mempools_hashmap.insert(TX_MEMPOOL_ID, tx_mempool);

        Ok(MemoryManager {
            mempools: mempools_hashmap,
            _next_id_to_allocate: 2,
            address_cache_2mb,
            address_cache_4kb,
            address_cache_1gb,
        })
    }

    #[inline]
    fn find_mempool_id(&self, buf: &[u8]) -> Option<MempoolID> {
        match self.address_cache_2mb.get(&closest_2mb_page(buf.as_ptr())) {
            Some(m) => {
                return Some(*m);
            },
            None => {},
        }
        match self.address_cache_4kb.get(&closest_4k_page(buf.as_ptr())) {
            Some(m) => {
                return Some(*m);
            },
            None => {},
        }
        match self.address_cache_1gb.get(&closest_1g_page(buf.as_ptr())) {
            Some(m) => {
                return Some(*m);
            },
            None => {},
        }
        return None;
    }

    pub fn recover_metadata(&self, ptr: &[u8]) -> Result<Option<datapath_metadata_t>, Fail> {
        debug!("Calling recover with ptr: {:?}", ptr.as_ptr());
        if let Some(id) = self.find_mempool_id(ptr) {
            debug!("Mempool id: {}, rx id: {}", id, RX_MEMPOOL_ID);
            let mempool = self.mempools.get(&id).unwrap();
            unsafe {
                return Ok(Some(mempool.recover_metadata_mbuf(ptr)));
            }
        } else {
            return Ok(None);
        }
    }

    pub fn alloc_buffer(&self, _size: usize) -> Result<Option<datapath_buffer_t>, Fail> {
        unimplemented!();
    }

    pub fn alloc_tx_buffer(&self) -> Result<Option<(datapath_buffer_t, usize)>, Fail> {
        let mempool = self.mempools.get(&TX_MEMPOOL_ID).unwrap();
        let buf = match mempool.alloc_buf()? {
            Some(buf) => buf,
            None => {
                return Ok(None);
            },
        };
        let buf_size = unsafe { access!(mempool.data_mempool(), item_len) };
        return Ok(Some((buf, buf_size as _)));
    }
}
