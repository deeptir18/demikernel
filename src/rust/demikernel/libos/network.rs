// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

//======================================================================================================================
// Imports
//======================================================================================================================

use crate::runtime::{
    fail::Fail,
    types::{
        datapath_buffer_t,
        datapath_metadata_t,
        demi_qresult_t,
        demi_sgarray_t,
        MempoolId,
    },
    QDesc,
    QToken,
};
use std::{
    net::SocketAddrV4,
    time::SystemTime,
};
use crate::cornflakes::{
    HybridSgaHdr,
    CopyContext,
};

#[cfg(feature = "catcollar-libos")]
use crate::catcollar::CatcollarLibOS;
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
}

pub type MsgID = u32;
pub type ConnID = usize;

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
        }
    }

    /// Pops data from a socket. Writes result into datapath metadata. Should take care of waiting for the packet too.
    /// return receivedPkt?
    pub fn pop_metadata(&mut self, sockqd: QDesc) -> Result<QToken, Fail> {
        unimplemented!();
    }

    /// Will change depending on the cornflakes API
    /// Pushes a vector of metadata objects to send with scatter-gather.
    pub fn push_metadata_vec(&self, sockqd: QDesc, metadata_vec: &Vec<datapath_metadata_t>) -> Result<QToken, Fail> {
        unimplemented!();
    }

    /// Recovers metadata from an arbitrary pointer.
    pub fn recover_metadata(&self, ptr: &[u8]) -> Result<Option<datapath_metadata_t>, Fail> {
        unimplemented!();
    }

    /// Turns datapath buffer into metadata object.
    pub fn get_metadata_from_buffer(&self, buffer: datapath_buffer_t) -> Result<datapath_metadata_t, Fail> {
        unimplemented!();
    }

    /// Adds a memory pool in datapath's underlying allocator.
    pub fn add_memory_pool(&self, size: usize, min_elts: usize) -> Result<MempoolId, Fail> {
        unimplemented!();
    }

    /// Allocates buffer for application to use.
    pub fn allocate_buffer(&mut self, size: usize) -> Result<Option<datapath_buffer_t>, Fail> {
        unimplemented!();
    }

    /// Allocates tx buffer for application to use.
    pub fn allocate_tx_buffer(&mut self) -> Result<(Option<datapath_buffer_t>, usize), Fail> {
        unimplemented!();
    }

    /// Decrements ref count or drops datapath buffer manually.
    pub fn drop_buffer(&mut self, datapath_buffer: datapath_buffer_t) -> Result<(), Fail> {
        unimplemented!();
    }

    /// Decrements ref count on underlying datapath buffer and drops if necessary.
    pub fn drop_metadata_vec(&mut self, metadata: datapath_metadata_t) -> Result<(), Fail> {
        unimplemented!();
    }

    /// Clones underlying metadata and increments the reference count.
    pub fn clone_metadata(&self, datapath_metadata: &datapath_metadata_t) -> Result<datapath_metadata_t, Fail> {
        unimplemented!();
    }

    /// Turns ref to datapath buffer, offset and length into metadata object.
    pub fn get_metadata_from_tx_buffer(&self, buf: &datapath_buffer_t, offset: usize, len: usize) -> Result<datapath_metadata_t, Fail> {
        unimplemented!();
    }

    pub fn push_cornflakes_obj(
        &mut self,
        sockqd: QDesc, 
        _copy_context: &mut CopyContext,
        _cornflakes_obj: &impl HybridSgaHdr,
    ) -> Result<QToken, Fail> {
        unimplemented!();
    }

    pub fn release_cornflakes_obj(
        &mut self,
        _copy_context: &mut CopyContext,
        _cornflakes_obj: impl HybridSgaHdr,
    ) -> Result<(), Fail> {
        unimplemented!();
    }
    
    pub fn get_copying_threshold(&self) -> usize {
        unimplemented!();
    }
}
