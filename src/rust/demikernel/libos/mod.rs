// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.
#![allow(deprecated)]

pub mod name;
pub mod network;

//======================================================================================================================
// Imports
//======================================================================================================================

use self::{
    name::LibOSName,
    network::{
        NetworkLibOS,
        OperationResult,
    },
};
use crate::{
    cornflakes::{
        CopyContext,
        ObjEnum,
    },
    demikernel::config::Config,
    runtime::{
        fail::Fail,
        logging,
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
    env,
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
// Structures
//======================================================================================================================

/// LibOS
pub enum LibOS {
    /// Network LibOS
    NetworkLibOS(NetworkLibOS),
}

//======================================================================================================================
// Associated Functions
//======================================================================================================================

/// Associated functions for LibOS.
impl LibOS {
    /// Instantiates a new LibOS.
    pub fn new(libos_name: LibOSName) -> Result<Self, Fail> {
        logging::initialize();

        // Read in configuration file.
        let config_path: String = match env::var("CONFIG_PATH") {
            Ok(config_path) => config_path,
            Err(_) => {
                return Err(Fail::new(
                    libc::EINVAL,
                    "missing value for CONFIG_PATH environment variable",
                ))
            },
        };
        let config: Config = Config::new(config_path);

        // Instantiate LibOS.
        #[allow(unreachable_patterns)]
        let libos: LibOS = match libos_name {
            #[cfg(feature = "catnap-libos")]
            LibOSName::Catnap => Self::NetworkLibOS(NetworkLibOS::Catnap(CatnapLibOS::new(&config))),
            #[cfg(feature = "catcollar-libos")]
            LibOSName::Catcollar => Self::NetworkLibOS(NetworkLibOS::Catcollar(CatcollarLibOS::new(&config))),
            #[cfg(feature = "catpowder-libos")]
            LibOSName::Catpowder => Self::NetworkLibOS(NetworkLibOS::Catpowder(CatpowderLibOS::new(&config))),
            #[cfg(feature = "catnip-libos")]
            LibOSName::Catnip => Self::NetworkLibOS(NetworkLibOS::Catnip(CatnipLibOS::new(&config))),
            #[cfg(feature = "catcorn-libos")]
            LibOSName::Catcorn => Self::NetworkLibOS(NetworkLibOS::Catcorn(
                CatcornLibOS::new(&config).expect("Failed to init catcorn libos"),
            )),
            _ => panic!("unsupported libos"),
        };

        Ok(libos)
    }

    /// Waits on a pending operation in an I/O queue.
    #[deprecated]
    pub fn wait_any2(&mut self, qts: &[QToken]) -> Result<(usize, QDesc, OperationResult), Fail> {
        match self {
            LibOS::NetworkLibOS(libos) => libos.wait_any2(qts),
        }
    }

    /// Waits on a pending operation in an I/O queue
    #[deprecated]
    pub fn wait2(&mut self, qt: QToken) -> Result<(QDesc, OperationResult), Fail> {
        match self {
            LibOS::NetworkLibOS(libos) => libos.wait2(qt),
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
            LibOS::NetworkLibOS(libos) => libos.socket(domain, socket_type, protocol),
        }
    }

    /// Binds a socket to a local address.
    pub fn bind(&mut self, sockqd: QDesc, local: SocketAddrV4) -> Result<(), Fail> {
        match self {
            LibOS::NetworkLibOS(libos) => libos.bind(sockqd, local),
        }
    }

    /// Marks a socket as a passive one.
    pub fn listen(&mut self, sockqd: QDesc, backlog: usize) -> Result<(), Fail> {
        match self {
            LibOS::NetworkLibOS(libos) => libos.listen(sockqd, backlog),
        }
    }

    /// Accepts an incoming connection on a TCP socket.
    pub fn accept(&mut self, sockqd: QDesc) -> Result<QToken, Fail> {
        match self {
            LibOS::NetworkLibOS(libos) => libos.accept(sockqd),
        }
    }

    /// Initiates a connection with a remote TCP pper.
    pub fn connect(&mut self, sockqd: QDesc, remote: SocketAddrV4) -> Result<QToken, Fail> {
        match self {
            LibOS::NetworkLibOS(libos) => libos.connect(sockqd, remote),
        }
    }

    /// Closes a socket.
    pub fn close(&mut self, qd: QDesc) -> Result<(), Fail> {
        match self {
            LibOS::NetworkLibOS(libos) => libos.close(qd),
        }
    }

    /// Pushes a scatter-gather array to a TCP socket.
    pub fn push(&mut self, qd: QDesc, sga: &demi_sgarray_t) -> Result<QToken, Fail> {
        match self {
            LibOS::NetworkLibOS(libos) => libos.push(qd, sga),
        }
    }

    /// Pushes raw data to a TCP socket.
    #[deprecated]
    pub fn push2(&mut self, qd: QDesc, data: &[u8]) -> Result<QToken, Fail> {
        match self {
            LibOS::NetworkLibOS(libos) => libos.push2(qd, data),
        }
    }

    /// Pushes a scatter-gather array to a UDP socket.
    pub fn pushto(&mut self, qd: QDesc, sga: &demi_sgarray_t, to: SocketAddrV4) -> Result<QToken, Fail> {
        match self {
            LibOS::NetworkLibOS(libos) => libos.pushto(qd, sga, to),
        }
    }

    /// Pushes raw data to a UDP socket.
    #[deprecated]
    pub fn pushto2(&mut self, qd: QDesc, data: &[u8], remote: SocketAddrV4) -> Result<QToken, Fail> {
        match self {
            LibOS::NetworkLibOS(libos) => libos.pushto2(qd, data, remote),
        }
    }

    /// Pops data from a socket.
    pub fn pop(&mut self, qd: QDesc) -> Result<QToken, Fail> {
        match self {
            LibOS::NetworkLibOS(libos) => libos.pop(qd),
        }
    }

    /// Waits for a pending operation in an I/O queue.
    pub fn wait(&mut self, qt: QToken) -> Result<demi_qresult_t, Fail> {
        match self {
            LibOS::NetworkLibOS(libos) => libos.wait(qt),
        }
    }

    /// Waits for an I/O operation to complete or a timeout to expire.
    pub fn timedwait(&mut self, qt: QToken, abstime: Option<SystemTime>) -> Result<demi_qresult_t, Fail> {
        match self {
            LibOS::NetworkLibOS(libos) => libos.timedwait(qt, abstime),
        }
    }

    /// Waits for any operation in an I/O queue.
    pub fn wait_any(&mut self, qts: &[QToken]) -> Result<(usize, demi_qresult_t), Fail> {
        match self {
            LibOS::NetworkLibOS(libos) => libos.wait_any(qts),
        }
    }

    /// Allocates a scatter-gather array.
    pub fn sgaalloc(&self, size: usize) -> Result<demi_sgarray_t, Fail> {
        match self {
            LibOS::NetworkLibOS(libos) => libos.sgaalloc(size),
        }
    }

    /// Releases a scatter-gather array.
    pub fn sgafree(&self, sga: demi_sgarray_t) -> Result<(), Fail> {
        match self {
            LibOS::NetworkLibOS(libos) => libos.sgafree(sga),
        }
    }

    /// Recovers metadata from an arbitrary pointer.
    pub fn recover_metadata(&self, ptr: &[u8]) -> Result<Option<datapath_metadata_t>, Fail> {
        match self {
            LibOS::NetworkLibOS(libos) => libos.recover_metadata(ptr),
        }
    }

    /// Adds a memory pool in datapath's underlying allocator.
    pub fn add_memory_pool(&self, size: usize, min_elts: usize) -> Result<MempoolID, Fail> {
        match self {
            LibOS::NetworkLibOS(libos) => libos.add_memory_pool(size, min_elts),
        }
    }

    /// Allocates buffer for application to use.
    pub fn allocate_buffer(&mut self, size: usize) -> Result<Option<datapath_buffer_t>, Fail> {
        match self {
            LibOS::NetworkLibOS(libos) => libos.allocate_buffer(size),
        }
    }

    /// Allocates tx buffer for application to use.
    pub fn allocate_tx_buffer(&mut self) -> Result<Option<(datapath_buffer_t, usize)>, Fail> {
        match self {
            LibOS::NetworkLibOS(libos) => libos.allocate_tx_buffer(),
        }
    }

    pub fn push_cornflakes_obj(
        &mut self,
        sockqd: QDesc,
        copy_context: CopyContext,
        cornflakes_obj: ObjEnum,
    ) -> Result<QToken, Fail> {
        match self {
            LibOS::NetworkLibOS(libos) => libos.push_cornflakes_obj(sockqd, copy_context, cornflakes_obj),
        }
    }

    pub fn push_slice(&mut self, sockqd: QDesc, slice: &[u8]) -> Result<QToken, Fail> {
        match self {
            LibOS::NetworkLibOS(libos) => libos.push_slice(sockqd, slice),
        }
    }

    pub fn push_metadata(&mut self, sockqd: QDesc, metadata: datapath_metadata_t) -> Result<QToken, Fail> {
        match self {
            LibOS::NetworkLibOS(libos) => libos.push_metadata(sockqd, metadata),
        }
    }

    pub fn get_copying_threshold(&self) -> usize {
        match self {
            LibOS::NetworkLibOS(libos) => libos.get_copying_threshold(),
        }
    }

    pub fn set_copying_threshold(&mut self, s: usize) {
        match self {
            LibOS::NetworkLibOS(libos) => libos.set_copying_threshold(s),
        }
    }
}
