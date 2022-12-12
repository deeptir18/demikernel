// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

//======================================================================================================================
// Imports
//======================================================================================================================
#![allow(deprecated)]
use crate::{
    cornflakes::{
        CopyContext,
        ObjEnum,
    },
    runtime::{
        fail::Fail,
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
};
use std::{
    net::SocketAddrV4,
    time::SystemTime,
};

#[cfg(feature = "catcollar-libos")]
use crate::catcollar::CatcollarLibOS;
#[cfg(feature = "catcorn-libos")]
use crate::catcorn::CatcornLibOS;
#[cfg(feature = "catnap-libos")]
use crate::catnap::CatnapLibOS;
#[cfg(feature = "catnip-libos")]
use crate::catnip::CatnipLibOS;
#[cfg(feature = "catpowder-libos")]
use crate::catpowder::CatpowderLibOS;

//======================================================================================================================
// Exports
//======================================================================================================================

pub use crate::inetstack::operations::OperationResult;

//======================================================================================================================
// Structures
//======================================================================================================================

/// Network LIBOS.
pub enum NetworkLibOS {
    #[cfg(feature = "catpowder-libos")]
    Catpowder(CatpowderLibOS),
    #[cfg(feature = "catnap-libos")]
    Catnap(CatnapLibOS),
    #[cfg(feature = "catcollar-libos")]
    Catcollar(CatcollarLibOS),
    #[cfg(feature = "catnip-libos")]
    Catnip(CatnipLibOS),
    #[cfg(feature = "catcorn-libos")]
    Catcorn(CatcornLibOS),
}

//======================================================================================================================
// Associated Functions
//======================================================================================================================

/// Associated functions for network LibOSes.
impl NetworkLibOS {
    /// Waits on a pending operation in an I/O queue.
    #[deprecated]
    pub fn wait_any2(&mut self, qts: &[QToken]) -> Result<(usize, QDesc, OperationResult), Fail> {
        match self {
            #[cfg(feature = "catpowder-libos")]
            NetworkLibOS::Catpowder(libos) => libos.wait_any2(qts),
            #[cfg(feature = "catnap-libos")]
            NetworkLibOS::Catnap(libos) => libos.wait_any2(qts),
            #[cfg(feature = "catcollar-libos")]
            NetworkLibOS::Catcollar(libos) => libos.wait_any2(qts),
            #[cfg(feature = "catnip-libos")]
            NetworkLibOS::Catnip(libos) => libos.wait_any2(qts),
            #[cfg(feature = "catcorn-libos")]
            NetworkLibOS::Catcorn(libos) => libos.wait_any2(qts),
        }
    }

    /// Waits on a pending operation in an I/O queue.
    #[deprecated]
    pub fn wait2(&mut self, qt: QToken) -> Result<(QDesc, OperationResult), Fail> {
        match self {
            #[cfg(feature = "catpowder-libos")]
            NetworkLibOS::Catpowder(libos) => libos.wait2(qt),
            #[cfg(feature = "catnap-libos")]
            NetworkLibOS::Catnap(libos) => libos.wait2(qt),
            #[cfg(feature = "catcollar-libos")]
            NetworkLibOS::Catcollar(libos) => libos.wait2(qt),
            #[cfg(feature = "catnip-libos")]
            NetworkLibOS::Catnip(libos) => libos.wait2(qt),
            #[cfg(feature = "catcorn-libos")]
            NetworkLibOS::Catcorn(libos) => libos.wait2(qt),
        }
    }

    /// Creates a socket.
    pub fn socket(
        &mut self,
        domain: libc::c_int,
        socket_type: libc::c_int,
        protocol: libc::c_int,
    ) -> Result<QDesc, Fail> {
        match self {
            #[cfg(feature = "catpowder-libos")]
            NetworkLibOS::Catpowder(libos) => libos.socket(domain, socket_type, protocol),
            #[cfg(feature = "catnap-libos")]
            NetworkLibOS::Catnap(libos) => libos.socket(domain, socket_type, protocol),
            #[cfg(feature = "catcollar-libos")]
            NetworkLibOS::Catcollar(libos) => libos.socket(domain, socket_type, protocol),
            #[cfg(feature = "catnip-libos")]
            NetworkLibOS::Catnip(libos) => libos.socket(domain, socket_type, protocol),
            #[cfg(feature = "catcorn-libos")]
            NetworkLibOS::Catcorn(libos) => libos.socket(domain, socket_type, protocol),
        }
    }

    /// Binds a socket to a local address.
    pub fn bind(&mut self, sockqd: QDesc, local: SocketAddrV4) -> Result<(), Fail> {
        match self {
            #[cfg(feature = "catpowder-libos")]
            NetworkLibOS::Catpowder(libos) => libos.bind(sockqd, local),
            #[cfg(feature = "catnap-libos")]
            NetworkLibOS::Catnap(libos) => libos.bind(sockqd, local),
            #[cfg(feature = "catcollar-libos")]
            NetworkLibOS::Catcollar(libos) => libos.bind(sockqd, local),
            #[cfg(feature = "catnip-libos")]
            NetworkLibOS::Catnip(libos) => libos.bind(sockqd, local),
            #[cfg(feature = "catcorn-libos")]
            NetworkLibOS::Catcorn(libos) => libos.bind(sockqd, local),
        }
    }

    /// Marks a socket as a passive one.
    pub fn listen(&mut self, sockqd: QDesc, backlog: usize) -> Result<(), Fail> {
        match self {
            #[cfg(feature = "catpowder-libos")]
            NetworkLibOS::Catpowder(libos) => libos.listen(sockqd, backlog),
            #[cfg(feature = "catnap-libos")]
            NetworkLibOS::Catnap(libos) => libos.listen(sockqd, backlog),
            #[cfg(feature = "catcollar-libos")]
            NetworkLibOS::Catcollar(libos) => libos.listen(sockqd, backlog),
            #[cfg(feature = "catnip-libos")]
            NetworkLibOS::Catnip(libos) => libos.listen(sockqd, backlog),
            #[cfg(feature = "catcorn-libos")]
            NetworkLibOS::Catcorn(libos) => libos.listen(sockqd, backlog),
        }
    }

    /// Accepts an incoming connection on a TCP socket.
    pub fn accept(&mut self, sockqd: QDesc) -> Result<QToken, Fail> {
        match self {
            #[cfg(feature = "catpowder-libos")]
            NetworkLibOS::Catpowder(libos) => libos.accept(sockqd),
            #[cfg(feature = "catnap-libos")]
            NetworkLibOS::Catnap(libos) => libos.accept(sockqd),
            #[cfg(feature = "catcollar-libos")]
            NetworkLibOS::Catcollar(libos) => libos.accept(sockqd),
            #[cfg(feature = "catnip-libos")]
            NetworkLibOS::Catnip(libos) => libos.accept(sockqd),
            #[cfg(feature = "catcorn-libos")]
            NetworkLibOS::Catcorn(libos) => libos.accept(sockqd),
        }
    }

    /// Initiates a connection with a remote TCP pper.
    pub fn connect(&mut self, sockqd: QDesc, remote: SocketAddrV4) -> Result<QToken, Fail> {
        match self {
            #[cfg(feature = "catpowder-libos")]
            NetworkLibOS::Catpowder(libos) => libos.connect(sockqd, remote),
            #[cfg(feature = "catnap-libos")]
            NetworkLibOS::Catnap(libos) => libos.connect(sockqd, remote),
            #[cfg(feature = "catcollar-libos")]
            NetworkLibOS::Catcollar(libos) => libos.connect(sockqd, remote),
            #[cfg(feature = "catnip-libos")]
            NetworkLibOS::Catnip(libos) => libos.connect(sockqd, remote),
            #[cfg(feature = "catcorn-libos")]
            NetworkLibOS::Catcorn(libos) => libos.connect(sockqd, remote),
        }
    }

    /// Closes a socket.
    pub fn close(&mut self, sockqd: QDesc) -> Result<(), Fail> {
        match self {
            #[cfg(feature = "catpowder-libos")]
            NetworkLibOS::Catpowder(libos) => libos.close(sockqd),
            #[cfg(feature = "catnap-libos")]
            NetworkLibOS::Catnap(libos) => libos.close(sockqd),
            #[cfg(feature = "catcollar-libos")]
            NetworkLibOS::Catcollar(libos) => libos.close(sockqd),
            #[cfg(feature = "catnip-libos")]
            NetworkLibOS::Catnip(libos) => libos.close(sockqd),
            #[cfg(feature = "catcorn-libos")]
            NetworkLibOS::Catcorn(libos) => libos.close(sockqd),
        }
    }

    /// Pushes a scatter-gather array to a TCP socket.
    pub fn push(&mut self, sockqd: QDesc, sga: &demi_sgarray_t) -> Result<QToken, Fail> {
        match self {
            #[cfg(feature = "catpowder-libos")]
            NetworkLibOS::Catpowder(libos) => libos.push(sockqd, sga),
            #[cfg(feature = "catnap-libos")]
            NetworkLibOS::Catnap(libos) => libos.push(sockqd, sga),
            #[cfg(feature = "catcollar-libos")]
            NetworkLibOS::Catcollar(libos) => libos.push(sockqd, sga),
            #[cfg(feature = "catnip-libos")]
            NetworkLibOS::Catnip(libos) => libos.push(sockqd, sga),
            #[cfg(feature = "catcorn-libos")]
            NetworkLibOS::Catcorn(_libos) => {
                warn!("Push for demi_sgarray_t not implemented");
                unimplemented!();
            },
        }
    }

    /// Pushes raw data to a TCP socket.
    #[deprecated]
    pub fn push2(&mut self, sockqd: QDesc, data: &[u8]) -> Result<QToken, Fail> {
        match self {
            #[cfg(feature = "catpowder-libos")]
            NetworkLibOS::Catpowder(libos) => libos.push2(sockqd, data),
            #[cfg(feature = "catnap-libos")]
            NetworkLibOS::Catnap(libos) => libos.push2(sockqd, data),
            #[cfg(feature = "catcollar-libos")]
            NetworkLibOS::Catcollar(libos) => libos.push2(sockqd, data),
            #[cfg(feature = "catnip-libos")]
            NetworkLibOS::Catnip(libos) => libos.push2(sockqd, data),
            #[cfg(feature = "catcorn-libos")]
            NetworkLibOS::Catcorn(_libos) => {
                warn!("Push2 for demi_sgarray_t not implemented");
                unimplemented!();
            },
        }
    }

    /// Pushes a scatter-gather array to a UDP socket.
    pub fn pushto(&mut self, sockqd: QDesc, sga: &demi_sgarray_t, to: SocketAddrV4) -> Result<QToken, Fail> {
        match self {
            #[cfg(feature = "catpowder-libos")]
            NetworkLibOS::Catpowder(libos) => libos.pushto(sockqd, sga, to),
            #[cfg(feature = "catnap-libos")]
            NetworkLibOS::Catnap(libos) => libos.pushto(sockqd, sga, to),
            #[cfg(feature = "catcollar-libos")]
            NetworkLibOS::Catcollar(libos) => libos.pushto(sockqd, sga, to),
            #[cfg(feature = "catnip-libos")]
            NetworkLibOS::Catnip(libos) => libos.pushto(sockqd, sga, to),
            #[cfg(feature = "catcorn-libos")]
            NetworkLibOS::Catcorn(_libos) => {
                warn!("Pushto (udp) for demi_sgarray_t not implemented");
                unimplemented!();
            },
        }
    }

    /// Pushes raw data to a UDP socket.
    #[deprecated]
    pub fn pushto2(&mut self, sockqd: QDesc, data: &[u8], remote: SocketAddrV4) -> Result<QToken, Fail> {
        match self {
            #[cfg(feature = "catpowder-libos")]
            NetworkLibOS::Catpowder(libos) => libos.pushto2(sockqd, data, remote),
            #[cfg(feature = "catnap-libos")]
            NetworkLibOS::Catnap(libos) => libos.pushto2(sockqd, data, remote),
            #[cfg(feature = "catcollar-libos")]
            NetworkLibOS::Catcollar(libos) => libos.pushto2(sockqd, data, remote),
            #[cfg(feature = "catnip-libos")]
            NetworkLibOS::Catnip(libos) => libos.pushto2(sockqd, data, remote),
            #[cfg(feature = "catcorn-libos")]
            NetworkLibOS::Catcorn(_libos) => {
                warn!("Push2to (udp) for demi_sgarray_t not implemented");
                unimplemented!();
            },
        }
    }

    /// Pops data from a socket.
    pub fn pop(&mut self, sockqd: QDesc) -> Result<QToken, Fail> {
        match self {
            #[cfg(feature = "catpowder-libos")]
            NetworkLibOS::Catpowder(libos) => libos.pop(sockqd),
            #[cfg(feature = "catnap-libos")]
            NetworkLibOS::Catnap(libos) => libos.pop(sockqd),
            #[cfg(feature = "catcollar-libos")]
            NetworkLibOS::Catcollar(libos) => libos.pop(sockqd),
            #[cfg(feature = "catnip-libos")]
            NetworkLibOS::Catnip(libos) => libos.pop(sockqd),
            #[cfg(feature = "catcorn-libos")]
            NetworkLibOS::Catcorn(libos) => libos.pop(sockqd),
        }
    }

    /// Waits for a pending operation in an I/O queue.
    pub fn wait(&mut self, qt: QToken) -> Result<demi_qresult_t, Fail> {
        match self {
            #[cfg(feature = "catpowder-libos")]
            NetworkLibOS::Catpowder(libos) => libos.wait(qt),
            #[cfg(feature = "catnap-libos")]
            NetworkLibOS::Catnap(libos) => libos.wait(qt),
            #[cfg(feature = "catcollar-libos")]
            NetworkLibOS::Catcollar(libos) => libos.wait(qt),
            #[cfg(feature = "catnip-libos")]
            NetworkLibOS::Catnip(libos) => libos.wait(qt),
            #[cfg(feature = "catcorn-libos")]
            NetworkLibOS::Catcorn(libos) => libos.wait(qt),
        }
    }

    /// Waits for an I/O operation to complete or a timeout to expire.
    pub fn timedwait(&mut self, qt: QToken, abstime: Option<SystemTime>) -> Result<demi_qresult_t, Fail> {
        match self {
            #[cfg(feature = "catpowder-libos")]
            NetworkLibOS::Catpowder(libos) => libos.timedwait(qt, abstime),
            #[cfg(feature = "catnap-libos")]
            NetworkLibOS::Catnap(libos) => libos.timedwait(qt, abstime),
            #[cfg(feature = "catcollar-libos")]
            NetworkLibOS::Catcollar(libos) => libos.timedwait(qt, abstime),
            #[cfg(feature = "catnip-libos")]
            NetworkLibOS::Catnip(libos) => libos.timedwait(qt, abstime),
            #[cfg(feature = "catcorn-libos")]
            NetworkLibOS::Catcorn(libos) => libos.timedwait(qt, abstime),
        }
    }

    /// Waits for any operation in an I/O queue.
    pub fn wait_any(&mut self, qts: &[QToken]) -> Result<(usize, demi_qresult_t), Fail> {
        match self {
            #[cfg(feature = "catpowder-libos")]
            NetworkLibOS::Catpowder(libos) => libos.wait_any(qts),
            #[cfg(feature = "catnap-libos")]
            NetworkLibOS::Catnap(libos) => libos.wait_any(qts),
            #[cfg(feature = "catcollar-libos")]
            NetworkLibOS::Catcollar(libos) => libos.wait_any(qts),
            #[cfg(feature = "catnip-libos")]
            NetworkLibOS::Catnip(libos) => libos.wait_any(qts),
            #[cfg(feature = "catcorn-libos")]
            NetworkLibOS::Catcorn(libos) => libos.wait_any(qts),
        }
    }

    /// Allocates a scatter-gather array.
    pub fn sgaalloc(&self, size: usize) -> Result<demi_sgarray_t, Fail> {
        match self {
            #[cfg(feature = "catpowder-libos")]
            NetworkLibOS::Catpowder(libos) => libos.sgaalloc(size),
            #[cfg(feature = "catnap-libos")]
            NetworkLibOS::Catnap(libos) => libos.sgaalloc(size),
            #[cfg(feature = "catcollar-libos")]
            NetworkLibOS::Catcollar(libos) => libos.sgaalloc(size),
            #[cfg(feature = "catnip-libos")]
            NetworkLibOS::Catnip(libos) => libos.sgaalloc(size),
            #[cfg(feature = "catcorn-libos")]
            NetworkLibOS::Catcorn(_libos) => {
                warn!("Allocation of sgarray_t not implemented for catcorn");
                unimplemented!();
            },
        }
    }

    /// Releases a scatter-gather array.
    pub fn sgafree(&self, sga: demi_sgarray_t) -> Result<(), Fail> {
        match self {
            #[cfg(feature = "catpowder-libos")]
            NetworkLibOS::Catpowder(libos) => libos.sgafree(sga),
            #[cfg(feature = "catnap-libos")]
            NetworkLibOS::Catnap(libos) => libos.sgafree(sga),
            #[cfg(feature = "catcollar-libos")]
            NetworkLibOS::Catcollar(libos) => libos.sgafree(sga),
            #[cfg(feature = "catnip-libos")]
            NetworkLibOS::Catnip(libos) => libos.sgafree(sga),
            #[cfg(feature = "catcorn-libos")]
            NetworkLibOS::Catcorn(_libos) => {
                warn!("Free of sgarray_t not implemented for catcorn");
                unimplemented!();
            },
        }
    }

    /// Recovers metadata from an arbitrary pointer.
    pub fn recover_metadata(&self, ptr: &[u8]) -> Result<Option<datapath_metadata_t>, Fail> {
        match self {
            #[cfg(feature = "catcorn-libos")]
            NetworkLibOS::Catcorn(libos) => libos.recover_metadata(ptr),
            _ => {
                warn!("Recover metadata function only implemented for Catcorn.");
                unimplemented!();
            },
        }
    }

    /// Adds a memory pool in datapath's underlying allocator.
    pub fn add_memory_pool(&self, size: usize, min_elts: usize) -> Result<MempoolID, Fail> {
        match self {
            #[cfg(feature = "catcorn-libos")]
            NetworkLibOS::Catcorn(libos) => libos.add_memory_pool(size, min_elts),
            _ => {
                warn!("add memory pool function only implemented for catcorn.");
                unimplemented!();
            },
        }
    }

    /// Allocates buffer for application to use.
    pub fn allocate_buffer(&mut self, size: usize) -> Result<Option<datapath_buffer_t>, Fail> {
        match self {
            #[cfg(feature = "catcorn-libos")]
            NetworkLibOS::Catcorn(libos) => libos.allocate_buffer(size),
            _ => {
                warn!("Allocate buffer function only implemented for catcorn.");
                unimplemented!();
            },
        }
    }

    /// Allocates buffer for application to use.
    pub fn allocate_tx_buffer(&self) -> Result<Option<(datapath_buffer_t, usize)>, Fail> {
        match self {
            #[cfg(feature = "catcorn-libos")]
            NetworkLibOS::Catcorn(libos) => libos.allocate_tx_buffer(),
            _ => {
                warn!("Allocate tx buffer function only implemented for catcorn.");
                unimplemented!();
            },
        }
    }

    pub fn push_cornflakes_obj(
        &mut self,
        sockqd: QDesc,
        copy_context: CopyContext,
        cornflakes_obj: ObjEnum,
        pkt_timestamp: u64,
        flow_id: u64,
    ) -> Result<QToken, Fail> {
        match self {
            #[cfg(feature = "catcorn-libos")]
            NetworkLibOS::Catcorn(libos) => {
                libos.push_cornflakes_obj(sockqd, copy_context, cornflakes_obj, pkt_timestamp, flow_id)
            },
            _ => {
                warn!("push cornflakes obj only implemented for catcorn.");
                unimplemented!();
            },
        }
    }

    pub fn push_slice(
        &mut self,
        sockqd: QDesc,
        slice: &[u8],
        pkt_timestamp: u64,
        flow_id: u64,
    ) -> Result<QToken, Fail> {
        match self {
            #[cfg(feature = "catcorn-libos")]
            NetworkLibOS::Catcorn(libos) => libos.push_slice(sockqd, slice, pkt_timestamp, flow_id),
            _ => {
                warn!("push slice only implemented for catcorn.");
                unimplemented!();
            },
        }
    }

    pub fn push_metadata(&mut self, sockqd: QDesc, metadata: datapath_metadata_t) -> Result<QToken, Fail> {
        match self {
            #[cfg(feature = "catcorn-libos")]
            NetworkLibOS::Catcorn(libos) => libos.push_metadata(sockqd, metadata),
            _ => {
                warn!("push metadata only implemented for catcorn.");
                unimplemented!();
            },
        }
    }

    pub fn get_copying_threshold(&self) -> usize {
        match self {
            #[cfg(feature = "catcorn-libos")]
            NetworkLibOS::Catcorn(libos) => libos.get_copying_threshold(),
            _ => {
                warn!("get copying threshold only implemented for catcorn");
                unimplemented!();
            },
        }
    }

    pub fn set_copying_threshold(&mut self, t: usize) {
        match self {
            #[cfg(feature = "catcorn-libos")]
            NetworkLibOS::Catcorn(libos) => libos.set_copying_threshold(t),
            _ => {
                warn!("get copying threshold only implemented for catcorn");
                unimplemented!();
            },
        }
    }
}
