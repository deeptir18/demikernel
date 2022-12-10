// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

use crate::{
    catcorn::Mlx5Runtime,
    runtime::{
        memory::Buffer,
        types::{
            demi_accept_result_t,
            demi_opcode_t,
            demi_qr_value_t,
            demi_qresult_t,
        },
        QDesc,
    },
    OperationResult,
};
use std::{
    mem,
    rc::Rc,
};

pub fn pack_result(_rt: Rc<Mlx5Runtime>, result: OperationResult, qd: QDesc, qt: u64) -> demi_qresult_t {
    match result {
        OperationResult::Connect => demi_qresult_t {
            qr_opcode: demi_opcode_t::DEMI_OPC_CONNECT,
            qr_qd: qd.into(),
            qr_qt: qt,
            qr_value: unsafe { mem::zeroed() },
        },
        OperationResult::Accept(new_qd) => {
            let sin = unsafe { mem::zeroed() };
            let qr_value = demi_qr_value_t {
                ares: demi_accept_result_t {
                    qd: new_qd.into(),
                    addr: sin,
                },
            };
            demi_qresult_t {
                qr_opcode: demi_opcode_t::DEMI_OPC_ACCEPT,
                qr_qd: qd.into(),
                qr_qt: qt,
                qr_value,
            }
        },
        OperationResult::Push => demi_qresult_t {
            qr_opcode: demi_opcode_t::DEMI_OPC_PUSH,
            qr_qd: qd.into(),
            qr_qt: qt,
            qr_value: unsafe { mem::zeroed() },
        },
        OperationResult::Pop(addr, bytes) => {
            // turn the address Buffer into a datapath metadata t
            match bytes {
                Buffer::Heap(_dbuf) => {
                    warn!("Why is pop returning a heap allocated dbuf");
                    unimplemented!();
                },
                Buffer::CornflakesObj(_) => {
                    warn!("Ok there's like no way pop should have a cornflakes obj");
                    unimplemented!();
                },
                Buffer::MetadataObj(mut metadata) => {
                    debug!(
                        "Pop returned metadata obj with addr {:?}, offset {}, len {}",
                        metadata.buffer, metadata.offset, metadata.len
                    );
                    if let Some(endpoint) = addr {
                        let saddr: libc::sockaddr_in = {
                            // TODO: check the following byte order conversion.
                            libc::sockaddr_in {
                                sin_family: libc::AF_INET as u16,
                                sin_port: endpoint.port().into(),
                                sin_addr: libc::in_addr {
                                    s_addr: u32::from_le_bytes(endpoint.ip().octets()),
                                },
                                sin_zero: [0; 8],
                            }
                        };
                        metadata.metadata_addr =
                            Some(unsafe { mem::transmute::<libc::sockaddr_in, libc::sockaddr>(saddr) });
                    }

                    let metadata_drop = std::mem::ManuallyDrop::new(metadata);
                    let qr_value = demi_qr_value_t {
                        qr_metadata: metadata_drop,
                    };
                    demi_qresult_t {
                        qr_opcode: demi_opcode_t::DEMI_OPC_POP,
                        qr_qd: qd.into(),
                        qr_qt: qt,
                        qr_value,
                    }
                },
            }
        },
        OperationResult::Failed(e) => {
            warn!("Operation Failed: {:?}", e);
            demi_qresult_t {
                qr_opcode: demi_opcode_t::DEMI_OPC_FAILED,
                qr_qd: qd.into(),
                qr_qt: qt,
                qr_value: unsafe { mem::zeroed() },
            }
        },
    }
}
