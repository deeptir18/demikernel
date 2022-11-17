// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

use crate::{
    demikernel::libos::{
        LibOS,
        network::NetworkLibOS,
    },
    runtime::{
        fail::Fail,
        types::{
            datapath_metadata_t,
            datapath_buffer_t
        },
    },
};
use bitmaps::Bitmap;
use byteorder::{ByteOrder, LittleEndian};
use anyhow::{
    bail,
    Error,
};

//==============================================================================
// Cornflakes Objects
//==============================================================================

pub const SIZE_FIELD: usize = 4;
pub const OFFSET_FIELD: usize = 4;
/// u32 at beginning representing bitmap size in bytes
pub const BITMAP_LENGTH_FIELD: usize = 4;

struct ForwardPointer<'a>(&'a [u8], usize);

impl<'a> ForwardPointer<'a> {
    #[inline]
    pub fn get_size(&self) -> u32 {
        LittleEndian::read_u32(&self.0[self.1..(self.1 + 4)])
    }

    #[inline]
    pub fn get_offset(&self) -> u32 {
        LittleEndian::read_u32(&self.0[(self.1 + 4)..(self.1 + 8)])
    }
}

struct MutForwardPointer<'a>(&'a mut [u8], usize);

impl<'a> MutForwardPointer<'a> {
    #[inline]
    pub fn write_size(&mut self, size: u32) {
        LittleEndian::write_u32(&mut self.0[self.1..(self.1 + 4)], size);
    }

    #[inline]
    pub fn write_offset(&mut self, off: u32) {
        LittleEndian::write_u32(&mut self.0[(self.1 + 4)..(self.1 + 8)], off);
    }
}
// Copy Context
pub struct SerializationCopyBuf {
    buf: datapath_buffer_t,
    total_len: usize,
}

impl SerializationCopyBuf {
    pub fn new(network_lib_os: &mut NetworkLibOS) -> Result<Self, Error> {
        let (buf_option, max_len) = network_lib_os.allocate_tx_buffer().expect("Could not allocate tx buffer") ;

        match buf_option {
            Some(buf) => {
                // debug!(
                //     "Allocated new serialization copy buf, current length is {}",
                //     buf.as_ref().len()
                // );
                return Ok(SerializationCopyBuf {
                    buf: buf,
                    total_len: max_len,
                })
            }
            None => {
                bail!("Could not allocate tx buffer for serialization copying.")
            }
        };
    }
    #[inline]
    pub fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buf.write(buf)
    }

    #[inline]
    pub fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
    #[inline]
    pub fn len(&self) -> usize {
        self.buf.as_ref().len()
    }

    #[inline]
    pub fn remaining(&self) -> usize {
        self.total_len - self.len()
    }
    
    #[inline]
    pub fn copy_context_ref(
        &self,
        index: usize,
        start: usize,
        len: usize,
        total_offset: usize,
    ) -> CopyContextRef {
        debug!(
            "Copy context ref being made"
        );
        CopyContextRef::new(self.buf.clone(), index, start, len, total_offset)
    }
}

pub struct CopyContext {
    pub copy_buffers: Vec<SerializationCopyBuf>,
    threshold: usize,
    current_length: usize,
    remaining: usize,
}

impl CopyContext {
    #[inline]
    pub fn should_copy(&self, ptr: &[u8]) -> bool {
        ptr.len() < self.threshold
    }

    #[inline]
    pub fn data_len(&self) -> usize {
        self.copy_buffers.iter().map(|buf| buf.len()).sum::<usize>()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.copy_buffers.len()
    }

    #[inline]
    pub fn push(&mut self, network_lib_os: &mut NetworkLibOS) -> Result<(), Error> {
        let buf = SerializationCopyBuf::new(network_lib_os)?;
        self.remaining = buf.remaining();
        self.copy_buffers.push(buf);
        Ok(())
    }

    /// Copies data into copy context.
    /// Returns (start, end) range of copy context that buffer was copied into.
    #[inline]
    pub fn copy(&mut self, buf: &[u8], network_lib_os: &mut NetworkLibOS) -> Result<CopyContextRef, Error> {

        let current_length = self.current_length;
        // TODO: doesn't work if buffer is > than an MTU
        if self.remaining < buf.len() {
            self.push(network_lib_os)?;
        }
        let copy_buffers_len = self.copy_buffers.len();
        let last_buf = &mut self.copy_buffers[copy_buffers_len - 1];
        let current_offset = last_buf.len();
        let written = last_buf.write(buf)?;
        if written != buf.len() {
            bail!(
                "Failed to write entire buf len into copy buffer, only wrote: {:?}",
                written
            );
        }
        self.current_length += written;
        self.remaining -= written;
        return Ok(last_buf.copy_context_ref(
            copy_buffers_len - 1,
            current_offset,
            written,
            current_length,
        ));
    }
}
// TODO: (add doc)
pub struct CopyContextRef {
    // which buffer amongst the multiple mtu buffers
    // pointer to the index in the copy context array
    // TODO: (remove this field) 
    datapath_buffer: datapath_buffer_t,

    index: usize,
    total_offset: usize,
    // might be redundant
    start: usize,
    // from data
    len: usize,
}
impl CopyContextRef {
    pub fn new(
        datapath_buffer: datapath_buffer_t,
        index: usize,
        start: usize,
        len: usize,
        total_offset: usize,
    ) -> Self {
        CopyContextRef {
            datapath_buffer: datapath_buffer,
            index: index,
            start: start,
            len: len,
            total_offset: total_offset,
        }
    }
    fn as_ref(&self) -> &[u8] {
        &self.datapath_buffer.as_ref()[self.start..(self.start + self.len)]
    }

    #[inline]
    fn total_offset(&self) -> usize {
        self.total_offset
    }
    #[inline]
    fn datapath_buffer(&self) -> &datapath_buffer_t {
        &self.datapath_buffer
    }
    #[inline]
    fn index(&self) -> usize {
        self.index
    }
    #[inline]
    fn offset(&self) -> usize {
        self.start
    }
    #[inline]
    fn len(&self) -> usize {
        self.len
    }
}

// Basic byte array representation in Cornflakes
pub enum CFBytes {
    /// Either directly references a segment for zero-copy
    RefCounted(datapath_metadata_t),
    /// Or references the user provided copy context
    Copied(CopyContextRef),
    // Raw(&'raw [u8]),
}

type CallbackEntryState = ();

pub trait HybridSgaHdr: AsRef<[u8]> {
    const NUMBER_OF_FIELDS: usize = 1;

    const CONSTANT_HEADER_SIZE: usize = SIZE_FIELD + OFFSET_FIELD;

    const NUM_U32_BITMAPS: usize = 0;

    #[inline]
    fn num_zero_copy_scatter_gather_entries(&self) -> usize;
    
    fn get_bitmap_itermut(&mut self) -> std::slice::IterMut<Bitmap<32>> {
        [].iter_mut()
    }

    fn get_bitmap_iter(&self) -> std::slice::Iter<Bitmap<32>> {
        [].iter()
    }

    fn get_mut_bitmap_entry(&mut self, _offset: usize) -> &mut Bitmap<32> {
        unimplemented!();
    }

    fn get_bitmap_entry(&self, _offset: usize) -> &Bitmap<32> {
        unimplemented!();
    }

    fn set_bitmap(&mut self, _bitmap: impl Iterator<Item = Bitmap<32>>) {}

    #[inline]
    fn bitmap_length() -> usize {
        Self::NUM_U32_BITMAPS * 4
    }

    #[inline]
    fn get_bitmap_field(&self, field: usize, bitmap_offset: usize) -> bool {
        self.get_bitmap_entry(bitmap_offset).get(field)
    }

    #[inline]
    fn set_bitmap_field(&mut self, field: usize, bitmap_offset: usize) {
        self.get_mut_bitmap_entry(bitmap_offset).set(field, true);
    }

    #[inline]
    fn clear_bitmap(&mut self) {
        for bitmap in self.get_bitmap_itermut() {
            *bitmap &= Bitmap::<32>::new();
        }
    }

    fn serialize_bitmap(&self, header: &mut [u8], offset: usize) {
        LittleEndian::write_u32(
            &mut header[offset..(offset + BITMAP_LENGTH_FIELD)],
            Self::NUM_U32_BITMAPS as u32,
        );

        for (i, bitmap) in self.get_bitmap_iter().enumerate() {
            let slice = &mut header[(offset + BITMAP_LENGTH_FIELD + i * 4)
                ..(offset + BITMAP_LENGTH_FIELD + (i + 1) * 4)];
            slice.copy_from_slice(bitmap.as_bytes());
        }
    }

    #[inline]
    fn dynamic_header_size(&self) -> usize {
        0
    }

    #[inline]
    fn dynamic_header_start(&self) -> usize {
        0
    }
    /// Total header size.
    fn total_header_size(&self, with_ref: bool, _with_bitmap: bool) -> usize {
        Self::CONSTANT_HEADER_SIZE * (with_ref as usize)
            + self.dynamic_header_size()
    }
    // fn check_deep_equality(&self, other: &CFBytes) -> bool {
    //     self.len() == other.len() && self.as_ref().to_vec() == other.as_ref().to_vec()
    // }

    fn iterate_over_entries<F>(
        &self,
        _copy_context: &mut CopyContext,
        _header_len: usize,
        _header_buffer: &mut [u8],
        _constant_header_offset: usize,
        _dynamic_header_offset: usize,
        _cur_entry_ptr: &mut usize,
        _datapath_callback: &mut F,
        _callback_state: &mut CallbackEntryState,
    ) -> Result<usize, Error>
    where
        F: FnMut(&datapath_metadata_t, &mut CallbackEntryState) -> Result<(), Error>,
    {
        unimplemented!();
    }

    fn inner_serialize<'a>(
        &self,
        datapath: &mut NetworkLibOS,
        header_buffer: &mut [u8],
        constant_header_offset: usize,
        dynamic_header_offset: usize,
        copy_context: &mut CopyContext,
        zero_copy_entries: &mut [datapath_metadata_t],
        ds_offset: &mut usize,
    ) -> Result<(), Error>;

    #[inline]
    fn serialize_into_arena_datapath_sga<'a>(
        &self,
        datapath: &mut NetworkLibOS,
        mut copy_context: CopyContext,
        // arena: &'a bumpalo::Bump,
    ) -> Result<ArenaDatapathSga, Error> {
        debug!("Serializing into sga");
        let mut owned_hdr = {
            let size = self.total_header_size(false, true);
            Vec::with_capacity(size) 
            // bumpalo::collections::Vec::with_capacity_zeroed_in(size, arena)
        };
        let mut header_buffer = owned_hdr.as_mut_slice();
        let num_zero_copy_entries = self.num_zero_copy_scatter_gather_entries();
        let mut zero_copy_entries = Vec::from_iter(
            std::iter::repeat(datapath_metadata_t::default()).take(num_zero_copy_entries),
            // arena,
        );
        let mut ds_offset = header_buffer.len() + copy_context.data_len();

        // inner serialize
        self.inner_serialize(
            datapath,
            &mut header_buffer,
            0,
            self.dynamic_header_start(),
            &mut copy_context,
            zero_copy_entries.as_mut_slice(),
            &mut ds_offset,
        )?;

        Ok(ArenaDatapathSga::new(
            copy_context,
            zero_copy_entries,
            owned_hdr,
        ))
    }

    fn inner_deserialize(
        &mut self,
        buf: &datapath_metadata_t,
        header_offset: usize,
        buffer_offset: usize,
        // arena: &'arena bumpalo::Bump,
    ) -> Result<(), Error>;

    #[inline]
    fn deserialize(
        &mut self,
        pkt: &ReceivedPkt,
        offset: usize,
        // arena: &'arena bumpalo::Bump,
    ) -> Result<(), Error> {
        // Right now, for deserialize we assume one contiguous buffer
        let metadata = pkt.seg(0);
        self.inner_deserialize(metadata, 0, offset)?;
        Ok(())
    }
}

// #[derive(PartialEq, Eq)]
pub struct ArenaDatapathSga {
    // buffers user has copied into
    copy_context: CopyContext,
    // zero copy entries
    zero_copy_entries: Vec<datapath_metadata_t>,
    // actual hdr
    header: Vec<u8>,
}

impl ArenaDatapathSga {
    pub fn new(
        copy_context: CopyContext,
        zero_copy_entries: Vec<datapath_metadata_t>,
        header: Vec<u8>,
    ) -> Self {
        ArenaDatapathSga {
            copy_context: copy_context,
            zero_copy_entries: zero_copy_entries,
            header: header,
        }
    }
}

impl CFBytes {
    pub fn new(
        ptr: &[u8],
        network_lib_os: &mut NetworkLibOS,
        copy_context: &mut CopyContext,
    ) -> Self {
        if copy_context.should_copy(ptr) {
            let copy_context_ref = copy_context.copy(ptr, network_lib_os).expect("Could not copy buffers during CFBytes creation");
            return CFBytes::Copied(copy_context_ref);
        };

        match network_lib_os.recover_metadata(ptr).expect("Could not recover metadata") {
            Some(m) => CFBytes::RefCounted(m),
            None => CFBytes::Copied(copy_context.copy(ptr, network_lib_os).expect("Could not copy buffers during CFBytes creation")),
        }
    }

    fn as_ref(&self) -> &[u8] {
        match self {
            CFBytes::RefCounted(m) => m.as_ref(),
            CFBytes::Copied(copy_context_ref) => copy_context_ref.as_ref(),
        }
    }

    fn num_zero_copy_scatter_gather_entries(&self) -> usize {
        match self {
            CFBytes::RefCounted(_) => 1,
            CFBytes::Copied(_) => 0,
        }
    }
    
    #[inline]
    fn iterate_over_entries<F>(
        &self,
        _copy_context: &mut CopyContext,
        header_len: usize,
        header_buffer: &mut [u8],
        constant_header_offset: usize,
        _dynamic_header_offset: usize,
        cur_entry_ptr: &mut usize,
        datapath_callback: &mut F,
        callback_state: &mut CallbackEntryState,
    ) -> Result<usize, Error>
    where
        F: FnMut(&datapath_metadata_t, &mut CallbackEntryState) -> Result<(), Error>,
    {
        match self {
            CFBytes::RefCounted(metadata) => {
                // call the datapath callback on this metadata
                datapath_callback(&metadata, callback_state)?;
                let offset_to_write = *cur_entry_ptr;
                let object_len = metadata.as_ref().len();
                let mut obj_ref = MutForwardPointer(header_buffer, constant_header_offset);
                obj_ref.write_size(object_len as u32);
                obj_ref.write_offset(offset_to_write as u32);
                *cur_entry_ptr += object_len;
                Ok(object_len)
            }
            CFBytes::Copied(copy_context_ref) => {
                //copy_context.check(&copy_context_ref)?;
                // write in the offset and length into the correct location in the header buffer
                let offset_to_write = copy_context_ref.total_offset() + header_len;
                let mut obj_ref = MutForwardPointer(header_buffer, constant_header_offset);
                obj_ref.write_size(copy_context_ref.len() as u32);
                obj_ref.write_offset(offset_to_write as u32);
                // debug!(
                //     offset_to_write = offset_to_write,
                //     size = copy_context_ref.len(),
                //     copy_context_total_offset = copy_context_ref.total_offset(),
                //     header_buffer_len = header_buffer.len(),
                //     "Reached inner serialize for cf bytes"
                // );
                Ok(copy_context_ref.len())
            }
        }
    }

    #[inline]
    fn inner_serialize<'a>(
        &self,
        datapath: &mut NetworkLibOS,
        header_buffer: &mut [u8],
        constant_header_offset: usize,
        _dynamic_header_start: usize,
        copy_context: &mut CopyContext,
        zero_copy_scatter_gather_entries: &mut [datapath_metadata_t],
        ds_offset: &mut usize,
    ) -> Result<(), Error> {
        match self {
            CFBytes::RefCounted(metadata) => {
                zero_copy_scatter_gather_entries[0] = metadata.clone();
                let offset_to_write = *ds_offset;
                let mut obj_ref = MutForwardPointer(header_buffer, constant_header_offset);
                obj_ref.write_size(metadata.as_ref().len() as u32);
                obj_ref.write_offset(offset_to_write as u32);
                *ds_offset += metadata.as_ref().len();
            }
            CFBytes::Copied(copy_context_ref) => {
                // check the copy_context against the copy context ref
                //copy_context.check(&copy_context_ref)?;
                // write in the offset and length into the correct location in the header buffer
                let offset_to_write = copy_context_ref.total_offset() + header_buffer.len();
                let mut obj_ref = MutForwardPointer(header_buffer, constant_header_offset);
                obj_ref.write_size(copy_context_ref.len() as u32);
                obj_ref.write_offset(offset_to_write as u32);
                // tracing::debug!(
                //     constant_header_offset,
                //     offset_to_write,
                //     len = copy_context_ref.len(),
                //     "Filling in dpseg for copy context cf bytes"
                // );
            }
        }
        Ok(())
    }

    #[inline]
    fn inner_deserialize(
        &mut self,
        buf: &datapath_metadata_t,
        header_offset: usize,
        buffer_offset: usize,
        // _arena: &'arena bumpalo::Bump,
    ) -> Result<(), Error> {
        let mut new_metadata = buf.clone();
        let forward_pointer = ForwardPointer(buf.as_ref(), header_offset + buffer_offset);
        let original_offset = buf.offset();
        new_metadata.set_data_len_and_offset(
            forward_pointer.get_size() as usize,
            forward_pointer.get_offset() as usize + original_offset + buffer_offset,
        )?;
        *self = CFBytes::RefCounted(new_metadata);
        Ok(())
    }
}
// add serializers, add a new function, add drop, 


pub type MsgID = u32;
pub type ConnID = usize;

/// Received Packet data structure
pub struct ReceivedPkt {
    pkts: Vec<datapath_metadata_t>,
    id: MsgID,
    conn: ConnID,
}

impl ReceivedPkt {
    pub fn new(pkts: Vec<datapath_metadata_t>, id: MsgID, conn_id: ConnID) -> Self {
        ReceivedPkt {
            pkts: pkts,
            id: id,
            conn: conn_id,
        }
    }

    pub fn data_len(&self) -> usize {
        let sum: usize = self.pkts.iter().map(|pkt| pkt.data_len()).sum();
        sum
    }

    pub fn conn_id(&self) -> ConnID {
        self.conn
    }

    pub fn msg_id(&self) -> MsgID {
        self.id
    }

    pub fn num_segs(&self) -> usize {
        self.pkts.len()
    }

    pub fn seg(&self, idx: usize) -> &datapath_metadata_t {
        &self.pkts[idx]
    }

    pub fn iter(&self) -> std::slice::Iter<datapath_metadata_t> {
        self.pkts.iter()
    }

    pub fn iter_mut(&mut self) -> std::slice::IterMut<datapath_metadata_t> {
        self.pkts.iter_mut()
    }

    pub fn flatten(&self) -> Vec<u8> {
        let bytes: Vec<u8> = self
            .pkts
            .iter()
            .map(|pkt| pkt.as_ref().to_vec())
            .flatten()
            .collect();
        bytes
    }
}
