// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

//======================================================================================================================
// Imports
//======================================================================================================================

use anyhow::Result;
use demikernel::{
    cornflakes::{
        generated_objects::{
            ListCF,
            SingleBufferCF,
        },
        CFBytes,
        CopyContext,
        HybridSgaHdr,
        VariableList,
    },
    flatbuffers::echo_fb_generated::echo_fb::{
        SingleBufferFB,
        ListFB,
        SingleBufferFBArgs,
    },
    runtime::types::{
        demi_sgarray_t,
        datapath_metadata_t,
        datapath_buffer_t,
        datapath_metadata_t,
        demi_opcode_t,
    },
    LibOS,
    LibOSName,
    QDesc,
    QToken,
};

use byteorder::{
    BigEndian,
    ByteOrder,
};
use std::{
    env,
    net::SocketAddrV4,
    panic,
    slice,
    str::FromStr,
};

use flatbuffers::{root, FlatBufferBuilder, WIPOffset};
use std::marker::PhantomData;

#[cfg(target_os = "windows")]
pub const AF_INET: i32 = windows::Win32::Networking::WinSock::AF_INET.0 as i32;

#[cfg(target_os = "windows")]
pub const SOCK_STREAM: i32 = windows::Win32::Networking::WinSock::SOCK_STREAM as i32;

#[cfg(target_os = "linux")]
pub const AF_INET: i32 = libc::AF_INET;

#[cfg(target_os = "linux")]
pub const SOCK_STREAM: i32 = libc::SOCK_STREAM;

#[cfg(feature = "profiler")]
use demikernel::perftools::profiler;

pub enum ModeCodeT {
    MODE_CF = 0,
    MODE_FB,
    MODE_NONE,
}
//======================================================================================================================
// Constants
//======================================================================================================================

const BUFFER_SIZE: usize = 64;
const FILL_CHAR: u8 = 0x65;
pub const REQ_TYPE_SIZE: usize = 4;

//======================================================================================================================
// mksga()
//======================================================================================================================

// Makes a scatter-gather array.
fn mksga(libos: &mut LibOS, size: usize, value: u8) -> demi_sgarray_t {
    // Allocate scatter-gather array.
    let sga: demi_sgarray_t = match libos.sgaalloc(size) {
        Ok(sga) => sga,
        Err(e) => panic!("failed to allocate scatter-gather array: {:?}", e),
    };

    // Ensure that scatter-gather array has the requested size.
    assert!(sga.sga_segs[0].sgaseg_len as usize == size);

    // Fill in scatter-gather array.
    let ptr: *mut u8 = sga.sga_segs[0].sgaseg_buf as *mut u8;
    let len: usize = sga.sga_segs[0].sgaseg_len as usize;
    let slice: &mut [u8] = unsafe { slice::from_raw_parts_mut(ptr, len) };
    slice.fill(value);

    sga
}

pub enum SimpleMessageType {
    /// Message with a single field
    Single,
    /// List with a variable number of elements
    List(usize),
}

fn read_message_type(packet: &datapath_metadata_t) -> Result<SimpleMessageType> {
    let buf = &packet.as_ref();
    let msg_type = &buf[0..2];
    let size = &buf[2..4];

    match (BigEndian::read_u16(msg_type), BigEndian::read_u16(size)) {
        (0, 0) => Ok(SimpleMessageType::Single),
        (1, size) => Ok(SimpleMessageType::List(size as _)),
        (_, _) => {
            unimplemented!();
        },
    }
}

//======================================================================================================================
// server()
//======================================================================================================================
fn server(local: SocketAddrV4, mode: ModeCodeT) -> Result<()> {
    let libos_name: LibOSName = match LibOSName::from_env() {
        Ok(libos_name) => libos_name.into(),
        Err(e) => panic!("{:?}", e),
    };
    let mut libos: LibOS = match LibOS::new(libos_name) {
        Ok(libos) => libos,
        Err(e) => panic!("failed to initialize libos: {:?}", e.cause),
    };
    // Setup peer.
    let sockqd: QDesc = match libos.socket(AF_INET, SOCK_STREAM, 0) {
        Ok(qd) => qd,
        Err(e) => panic!("failed to create socket: {:?}", e.cause),
    };
    match libos.bind(sockqd, local) {
        Ok(()) => (),
        Err(e) => panic!("bind failed: {:?}", e.cause),
    };

    // Mark as a passive one.
    match libos.listen(sockqd, 16) {
        Ok(()) => (),
        Err(e) => panic!("listen failed: {:?}", e.cause),
    };

    let mut nr_pending: u64 = 0;
    let mut qtokens: Vec<QToken> = Vec::new();

    loop {
        if nr_pending < 1 {
            // Accept incoming connections.
            let qt: QToken = match libos.accept(sockqd) {
                Ok(qt) => qt,
                Err(e) => panic!("accept failed: {:?}", e.cause),
            };
            nr_pending += 1;
            qtokens.push(qt);
        }

        // The qresult has a datapath_metadata_t variable too alongside the sga_buffer optionally
        // so do we need to pop a vec of received packets, or is it ok to deserialize packet by packet?
        let (i, qr) = libos.wait_any(&qtokens).unwrap();
        qtokens.remove(i);

        // Parse the result.
        match qr.qr_opcode {
            demi_opcode_t::DEMI_OPC_ACCEPT => {
                // Pop first packet.
                let qd: QDesc = unsafe { qr.qr_value.ares.qd.into() };
                let qt: QToken = match libos.pop(qd) {
                    Ok(qt) => qt,
                    Err(e) => panic!("pop failed: {:?}", e.cause),
                };
                nr_pending -= 1;
                qtokens.push(qt);
            },
            // Pop completed.
            demi_opcode_t::DEMI_OPC_POP => {
                match mode {
                    // :::::::::::HANDLING CORNFLAKES ZERO COPY PACKETS::::::::::::::
                    ModeCodeT::MODE_CF => {
                        let qd: QDesc = qr.qr_qd.into();
                        let pkt_wrapper: std::mem::ManuallyDrop<datapath_metadata_t> =
                            unsafe { qr.qr_value.qr_metadata };
                        let pkt = std::mem::ManuallyDrop::<datapath_metadata_t>::into_inner(pkt_wrapper);
                        // Deserialize.
                        let mut copy_context = CopyContext::new(&mut libos)?;
                        let message_type = read_message_type(&pkt)?;

                        match message_type {
                            SimpleMessageType::Single => {
                                let mut single_deser = SingleBufferCF::new_in();
                                let mut single_ser = SingleBufferCF::new_in();
                                {
                                    single_deser.deserialize(&pkt, REQ_TYPE_SIZE)?;
                                }
                                {
                                    single_ser.set_message(CFBytes::new(
                                        single_deser.get_message().as_ref(),
                                        &mut libos,
                                        &mut copy_context,
                                    ));
                                    // tracing::debug!(set_msg =? single_ser.get_message().as_ref());
                                }
                                // Push data.
                                let qt: QToken = match libos.push_cornflakes_obj(qd, &mut copy_context, &mut single_ser)
                                {
                                    Ok(qt) => qt,
                                    Err(e) => panic!("failed to push CF object: {:?}", e),
                                };
                                qtokens.push(qt);
                                match libos.release_cornflakes_obj(&mut copy_context, single_ser) {
                                    Ok(_) => {},
                                    Err(e) => panic!("failed to release CF object: {:?}", e),
                                };
                            },
                            SimpleMessageType::List(_size) => {
                                unimplemented!();
                            },
                        }
                    },
                    // :::::::::::::::::::::::HANDLING NORMAL PACKETS:::::::::::::::::::
                    ModeCodeT::MODE_NONE => {
                        let qd: QDesc = qr.qr_qd.into();
                        let pkt: datapath_metadata_t = unsafe { qr.qr_value.qr_metadata };
        
                        // Push data.
                        let qt: QToken = match libos.push_metadata_t(qd, pkt) {
                            Ok(qt) => qt,
                            Err(e) => panic!("push failed: {:?}", e.cause),
                        };
                        qtokens.push(qt);
                        match libos.drop_metadata(pkt) {
                            Ok(_) => {},
                            Err(e) => panic!("failed to release scatter-gather array: {:?}", e),
                        }
                    },
                    // ::::::::::::::::::::::: HANDLING FLATBUFFERS :::::::::::::::::::::
                    ModeCodeT::MODE_FB => {
                        let qd: QDesc = qr.qr_qd.into();
                        let pkt: datapath_metadata_t = unsafe { qr.qr_value.qr_metadata };
                        let mut builder: flatbuffers::FlatBufferBuilder = flatbuffers::FlatBufferBuilder::new();
                        let msg_type = read_message_type(&pkt)?;
                        match msg_type {
                            SimpleMessageType::Single => {
                                let object_deser =
                                    root::<SingleBufferFB>(&pkt.as_ref()[REQ_TYPE_SIZE..])?;
                                let args = SingleBufferFBArgs {
                                    message: Some(
                                        builder
                                            .create_vector_direct::<u8>(object_deser.message().unwrap()),
                                    ),
                                };
                                let single_buffer_fb =
                                    SingleBufferFB::create(&mut builder, &args);
                                builder.finish(single_buffer_fb, None);
                            },
                            SimpleMessageType::List(_size) => {
                                unimplemented!();
                            },
                        }
                    },
                }
            },
            // Push completed.
            demi_opcode_t::DEMI_OPC_PUSH => {
                // Pop another packet.
                let qd: QDesc = qr.qr_qd.into();
                let qt: QToken = match libos.pop(qd) {
                    Ok(qt) => qt,
                    Err(e) => panic!("pop failed: {:?}", e.cause),
                };
                qtokens.push(qt);
            },
            demi_opcode_t::DEMI_OPC_FAILED => panic!("operation failed"),
            _ => panic!("unexpected result"),
        }
    }

    #[cfg(feature = "profiler")]
    profiler::write(&mut std::io::stdout(), None).expect("failed to write to stdout");

    // TODO: close socket when we get close working properly in catnip.
    Ok(())
}

//======================================================================================================================
// client()
//======================================================================================================================

fn client(remote: SocketAddrV4) -> Result<()> {
    let libos_name: LibOSName = match LibOSName::from_env() {
        Ok(libos_name) => libos_name.into(),
        Err(e) => panic!("{:?}", e),
    };
    let mut libos: LibOS = match LibOS::new(libos_name) {
        Ok(libos) => libos,
        Err(e) => panic!("failed to initialize libos: {:?}", e.cause),
    };
    let nrounds: usize = 1024;

    // Setup peer.
    let sockqd: QDesc = match libos.socket(AF_INET, SOCK_STREAM, 0) {
        Ok(qd) => qd,
        Err(e) => panic!("failed to create socket: {:?}", e.cause),
    };

    let qt: QToken = match libos.connect(sockqd, remote) {
        Ok(qt) => qt,
        Err(e) => panic!("connect failed: {:?}", e.cause),
    };
    match libos.wait(qt) {
        Ok(qr) if qr.qr_opcode == demi_opcode_t::DEMI_OPC_CONNECT => println!("connected!"),
        Err(e) => panic!("operation failed: {:?}", e),
        _ => panic!("unexpected result"),
    }

    // Issue n sends.
    for i in 0..nrounds {
        let sga: demi_sgarray_t = mksga(&mut libos, BUFFER_SIZE, FILL_CHAR);

        // Push data.
        let qt: QToken = match libos.push(sockqd, &sga) {
            Ok(qt) => qt,
            Err(e) => panic!("push failed: {:?}", e.cause),
        };
        match libos.wait(qt) {
            Ok(qr) if qr.qr_opcode == demi_opcode_t::DEMI_OPC_PUSH => (),
            Err(e) => panic!("operation failed: {:?}", e.cause),
            _ => panic!("unexpected result"),
        };
        match libos.sgafree(sga) {
            Ok(_) => {},
            Err(e) => panic!("failed to release scatter-gather array: {:?}", e),
        }

        // Pop data.
        let qt: QToken = match libos.pop(sockqd) {
            Ok(qt) => qt,
            Err(e) => panic!("pop failed: {:?}", e.cause),
        };
        let sga: demi_sgarray_t = match libos.wait(qt) {
            Ok(qr) if qr.qr_opcode == demi_opcode_t::DEMI_OPC_POP => unsafe { qr.qr_value.sga },
            Err(e) => panic!("operation failed: {:?}", e.cause),
            _ => panic!("unexpected result"),
        };

        // Sanity check received data.
        let ptr: *mut u8 = sga.sga_segs[0].sgaseg_buf as *mut u8;
        let len: usize = sga.sga_segs[0].sgaseg_len as usize;
        let slice: &mut [u8] = unsafe { slice::from_raw_parts_mut(ptr, len) };
        for x in slice {
            assert!(*x == FILL_CHAR);
        }

        match libos.sgafree(sga) {
            Ok(_) => {},
            Err(e) => panic!("failed to release scatter-gather array: {:?}", e),
        }

        println!("ping {:?}", i);
    }

    #[cfg(feature = "profiler")]
    profiler::write(&mut std::io::stdout(), None).expect("failed to write to stdout");

    // TODO: close socket when we get close working properly in catnip.
    Ok(())
}

//======================================================================================================================
// usage()
//======================================================================================================================

/// Prints program usage and exits.
fn usage(program_name: &String) {
    println!("Usage: {} MODE address\n", program_name);
    println!("Modes:\n");
    println!("  --client       Run program in client mode.\n");
    println!("  --server       Run program in server mode.\n");
    println!(
        "  --packet_type  Type of serialization format the packets should be created in \
         (cf_0c/cf_1c/flatbuffer/<None>).\n"
    );
}

//======================================================================================================================
// main()
//======================================================================================================================

fn convert(mode_name: String) -> ModeCodeT {
    if mode_name.contains("cf_0c") or mode_name.contains("cf_1c") {
        return ModeCodeT::MODE_CF;
    } else if mode_name.contains("flatbuffer") {
        return ModeCodeT::MODE_FB;
    }
    return ModeCodeT::MODE_NONE;
}

pub fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();

    let mut mode: ModeCodeT = ModeCodeT::MODE_NONE;
    if args.len() >= 5 {
        if args[3] == "--packet_type" {
            mode = convert(args[4].to_string());
        }
    }
    // println!("Mode {}", mode);
    if args.len() >= 3 {
        let sockaddr: SocketAddrV4 = SocketAddrV4::from_str(&args[2])?;
        if args[1] == "--server" {
            let ret: Result<()> = server(sockaddr, mode);
            return ret;
        } else if args[1] == "--client" {
            let ret: Result<()> = client(sockaddr);
            return ret;
        }
    }

    usage(&args[0]);

    Ok(())
}
