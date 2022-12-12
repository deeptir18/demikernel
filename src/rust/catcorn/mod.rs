// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.
//
//==============================================================================
// Imports
//==============================================================================
use self::{
    interop::pack_result,
    runtime::Mlx5Runtime,
};
use crate::{
    cornflakes::{
        CopyContext,
        ObjEnum,
    },
    demikernel::config::Config,
    inetstack::{
        operations::OperationResult,
        InetStack,
    },
    runtime::{
        fail::Fail,
        libmlx5::mlx5_bindings::custom_mlx5_err_to_str,
        memory::{
            Buffer,
            CornflakesObj,
        },
        timer::{
            Timer,
            TimerRc,
        },
        types::{
            datapath_buffer_t,
            datapath_metadata_t,
            demi_qresult_t,
            demi_sgarray_t,
            MempoolID,
        },
        QDesc,
        QToken,
    },
    scheduler::{
        Scheduler,
        SchedulerHandle,
    },
};
use std::{
    ffi::CStr,
    io::Write,
    net::SocketAddrV4,
    ops::{
        Deref,
        DerefMut,
    },
    rc::Rc,
    time::{
        Instant,
        SystemTime,
    },
};

#[cfg(feature = "profiler")]
use crate::{
    runtime::libmlx5::mlx5_bindings::custom_mlx5_err_to_str,
    timer,
};

pub unsafe fn check(func_name: &str, errno: ::std::os::raw::c_int) -> Result<(), Fail> {
    if errno != 0 {
        let c_buf = custom_mlx5_err_to_str(errno);
        let c_str: &CStr = CStr::from_ptr(c_buf);
        let str_slice: &str = c_str.to_str().unwrap();
        return Err(Fail::new(
            libc::EINVAL,
            &format!("Function {} failed from {} error", func_name, str_slice),
        ));
    }
    Ok(())
}

#[macro_export]
macro_rules! access(
    ($struct: expr, $field: ident) =>  {
        (*$struct).$field
    };
    ($struct: expr, $field: ident, $cast: ty) =>  {
        (*$struct).$field as $cast
    };
);

mod config;
mod interop;
mod memory;
pub mod runtime;

//==============================================================================
// Structures
//==============================================================================

/// Catcorn LibOS
pub struct CatcornLibOS {
    scheduler: Scheduler,
    inetstack: InetStack,
    rt: Rc<Mlx5Runtime>,
    copying_threshold: usize,
}

//==============================================================================
// Associate Functions
//==============================================================================

/// Associate Functions for Catcorn LibOS
impl CatcornLibOS {
    pub fn new(config: &Config) -> Result<Self, Fail> {
        let rt: Rc<Mlx5Runtime> = Rc::new(Mlx5Runtime::new(
            1,
            config.local_ipv4_addr(),
            config.local_mac_addr(),
            config.pci_addr(),
            config.arp_table(),
            config.disable_arp(),
            config.use_jumbo_frames(),
            config.mtu(),
            config.mss(),
            config.tcp_checksum_offload(),
            config.udp_checksum_offload(),
        )?);
        debug!(
            "Config use jumbo: {}, checksum off: {}",
            config.use_jumbo_frames(),
            config.tcp_checksum_offload()
        );
        let now: Instant = Instant::now();
        let clock: TimerRc = TimerRc(Rc::new(Timer::new(now)));
        let scheduler: Scheduler = Scheduler::default();
        let rng_seed: [u8; 32] = [0; 32];
        let inetstack: InetStack = InetStack::new(
            rt.clone(),
            scheduler.clone(),
            clock,
            rt.link_addr,
            rt.ipv4_addr,
            rt.udp_options.clone(),
            rt.tcp_options.clone(),
            rng_seed,
            rt.arp_options.clone(),
        )
        .unwrap();
        Ok(CatcornLibOS {
            inetstack,
            scheduler,
            rt,
            copying_threshold: 0,
        })
    }

    /// Create a push request for Demikernel to asynchronously write data from `sga` to the
    /// IO connection represented by `qd`. This operation returns immediately with a `QToken`.
    /// The data has been written when [`wait`ing](Self::wait) on the QToken returns.
    pub fn push(&mut self, _qd: QDesc, _sga: &demi_sgarray_t) -> Result<QToken, Fail> {
        unimplemented!();
        /*#[cfg(feature = "profiler")]
        timer!("catcorn::push");
        trace!("push(): qd={:?}", qd);
        match self.rt.clone_sgarray(sga) {
            Ok(buf) => {
                if buf.len() == 0 {
                    return Err(Fail::new(libc::EINVAL, "zero-length buffer"));
                }
                let future = self.do_push(qd, buf)?;
                let handle: SchedulerHandle = match self.scheduler.insert(future) {
                    Some(handle) => handle,
                    None => return Err(Fail::new(libc::EAGAIN, "cannot schedule co-routine")),
                };
                let qt: QToken = handle.into_raw().into();
                Ok(qt)
            },
            Err(e) => Err(e),
        }*/
    }

    pub fn pushto(&mut self, _qd: QDesc, _sga: &demi_sgarray_t, _to: SocketAddrV4) -> Result<QToken, Fail> {
        unimplemented!();
        /*#[cfg(feature = "profiler")]
        timer!("catnip::pushto");
        trace!("pushto2(): qd={:?}", qd);
        match self.rt.clone_sgarray(sga) {
            Ok(buf) => {
                if buf.len() == 0 {
                    return Err(Fail::new(libc::EINVAL, "zero-length buffer"));
                }
                let future = self.do_pushto(qd, buf, to)?;
                let handle: SchedulerHandle = match self.scheduler.insert(future) {
                    Some(handle) => handle,
                    None => return Err(Fail::new(libc::EAGAIN, "cannot schedule co-routine")),
                };
                let qt: QToken = handle.into_raw().into();
                Ok(qt)
            },
            Err(e) => Err(e),
        }*/
    }

    /// Waits for an operation to complete.
    pub fn wait(&mut self, qt: QToken) -> Result<demi_qresult_t, Fail> {
        #[cfg(feature = "profiler")]
        timer!("catnip::wait");
        trace!("wait(): qt={:?}", qt);

        let (qd, result): (QDesc, OperationResult) = self.wait2(qt)?;
        Ok(pack_result(self.rt.clone(), result, qd, qt.into()))
    }

    /// Waits for an I/O operation to complete or a timeout to expire.
    pub fn timedwait(&mut self, qt: QToken, abstime: Option<SystemTime>) -> Result<demi_qresult_t, Fail> {
        #[cfg(feature = "profiler")]
        timer!("catnip::timedwait");
        trace!("timedwait() qt={:?}, timeout={:?}", qt, abstime);

        let (qd, result): (QDesc, OperationResult) = self.timedwait2(qt, abstime)?;
        Ok(pack_result(self.rt.clone(), result, qd, qt.into()))
    }

    /// Waits for any operation to complete.
    pub fn wait_any(&mut self, qts: &[QToken]) -> Result<(usize, demi_qresult_t), Fail> {
        #[cfg(feature = "profiler")]
        timer!("catnip::wait_any");
        trace!("wait_any(): qts={:?}", qts);
        let (i, qd, r): (usize, QDesc, OperationResult) = self.wait_any2(qts)?;
        Ok((i, pack_result(self.rt.clone(), r, qd, qts[i].into())))
    }

    /// Allocates a scatter-gather array.
    pub fn sgaalloc(&self, _size: usize) -> Result<demi_sgarray_t, Fail> {
        unimplemented!();
        //self.rt.alloc_sgarray(size)
    }

    /// Releases a scatter-gather array.
    pub fn sgafree(&self, _sga: demi_sgarray_t) -> Result<(), Fail> {
        unimplemented!();
        //self.rt.free_sgarray(sga)
    }

    /// Recovers metadata from raw pointer.
    pub fn recover_metadata(&self, ptr: &[u8]) -> Result<Option<datapath_metadata_t>, Fail> {
        self.rt.recover_metadata(ptr)
    }

    pub fn add_memory_pool(&self, _size: usize, _min_elts: usize) -> Result<MempoolID, Fail> {
        unimplemented!();
    }

    pub fn allocate_buffer(&self, size: usize) -> Result<Option<datapath_buffer_t>, Fail> {
        self.rt.allocate_buffer(size)
    }

    pub fn allocate_tx_buffer(&self) -> Result<Option<(datapath_buffer_t, usize)>, Fail> {
        self.rt.allocate_tx_buffer()
    }

    pub fn push_metadata(&mut self, qd: QDesc, metadata: datapath_metadata_t) -> Result<QToken, Fail> {
        #[cfg(feature = "profiler")]
        timer!("catcorn::push");
        trace!("push(): qd={:?}", qd);
        let buffer_obj = Buffer::MetadataObj(metadata);
        let future = self.do_push(qd, buffer_obj)?;
        let handle: SchedulerHandle = match self.scheduler.insert(future) {
            Some(handle) => handle,
            None => return Err(Fail::new(libc::EAGAIN, "cannot schedule co-routine")),
        };
        let qt: QToken = handle.into_raw().into();
        Ok(qt)
    }

    pub fn push_cornflakes_obj(
        &mut self,
        qd: QDesc,
        copy_context: CopyContext,
        cornflakes_obj: ObjEnum,
        pkt_timestamp: u64,
        flow_id: u64,
    ) -> Result<QToken, Fail> {
        #[cfg(feature = "profiler")]
        timer!("catcorn::push_cornflakes_obj");
        trace!("push(): qd={:?}", qd);
        let buffer_obj =
            Buffer::CornflakesObj(CornflakesObj::new(cornflakes_obj, copy_context, pkt_timestamp, flow_id));
        let future = self.do_push(qd, buffer_obj)?;
        let handle: SchedulerHandle = match self.scheduler.insert(future) {
            Some(handle) => handle,
            None => return Err(Fail::new(libc::EAGAIN, "cannot schedule co-routine")),
        };
        let qt: QToken = handle.into_raw().into();
        Ok(qt)
    }

    pub fn push_slice(&mut self, qd: QDesc, slice: &[u8], pkt_timestamp: u64, flow_id: u64) -> Result<QToken, Fail> {
        #[cfg(feature = "profiler")]
        timer!("catcorn::push_slice");
        trace!("push(): qd={:?}", qd);
        let metadata = match self.allocate_tx_buffer()? {
            Some((mut b, _)) => {
                // write the pkt timestamp and flow id at the beginning of the packet
                b.write_u64(pkt_timestamp);
                b.write_u64(0);
                b.write_u64(flow_id);
                b.write_u64(0);
                b.write(slice)?;
                b.to_metadata(0, slice.len() + 32)
            },
            None => {
                return Err(Fail::new(libc::EAGAIN, "Could not allocate tx buffer to write slice"));
            },
        };
        let buffer_obj = Buffer::MetadataObj(metadata);
        let future = self.do_push(qd, buffer_obj)?;
        let handle: SchedulerHandle = match self.scheduler.insert(future) {
            Some(handle) => handle,
            None => return Err(Fail::new(libc::EAGAIN, "cannot schedule co-routine")),
        };
        let qt: QToken = handle.into_raw().into();
        Ok(qt)
    }

    pub fn set_copying_threshold(&mut self, s: usize) {
        self.copying_threshold = s;
    }

    pub fn get_copying_threshold(&self) -> usize {
        self.copying_threshold
    }
}

//==============================================================================
// Trait Implementations
//==============================================================================

/// De-Reference Trait Implementation for Catcorn LibOS
impl Deref for CatcornLibOS {
    type Target = InetStack;

    fn deref(&self) -> &Self::Target {
        &self.inetstack
    }
}

/// Mutable De-Reference Trait Implementation for Catcorn LibOS
impl DerefMut for CatcornLibOS {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inetstack
    }
}
