// Copyright (c) Microsoft Corporation
// Licensed under the MIT license.
pub mod generated_objects;

use crate::{
    runtime::{
        fail::Fail,
        types::{
            datapath_buffer_t,
            datapath_metadata_t,
        },
    },
    LibOS,
};
use bitmaps::Bitmap;
use byteorder::{
    ByteOrder,
    LittleEndian,
};
use generated_objects::{
    ListCF,
    SingleBufferCF,
};
use std::{
    io::Write,
    ops::Index,
    slice::Iter,
};

//==============================================================================
// Cornflakes Objects
//==============================================================================

pub enum ObjEnum {
    Single(SingleBufferCF),
    List(ListCF),
}

impl ObjEnum {
    pub fn total_header_size(&self) -> usize {
        match self {
            ObjEnum::Single(single) => single.total_header_size(false),
            ObjEnum::List(list) => list.total_header_size(false),
        }
    }

    pub fn total_length(&self, copy_context: &CopyContext) -> usize {
        match self {
            ObjEnum::Single(single) => single.total_length(copy_context),
            ObjEnum::List(list) => list.total_length(copy_context),
        }
    }

    pub fn num_segments_total(
        &self,
        with_header: bool,
        copy_context: &Vec<datapath_metadata_t>,
        ref_offset: usize,
        ref_length: usize,
    ) -> usize {
        match self {
            ObjEnum::Single(single) => single.num_segments_total(with_header, copy_context, ref_offset, ref_length),
            ObjEnum::List(list) => list.num_segments_total(with_header, copy_context, ref_offset, ref_length),
        }
    }

    pub fn write_header(
        &self,
        header_buffer: &mut [u8],
        copy_context: &Vec<datapath_metadata_t>,
        ref_offset: usize,
        ref_length: usize,
    ) -> usize {
        match self {
            ObjEnum::Single(single) => single.write_header(header_buffer, copy_context, ref_offset, ref_length),
            ObjEnum::List(list) => list.write_header(header_buffer, copy_context, ref_offset, ref_length),
        }
    }

    pub fn iterate_over_entries_with_callback<F, C>(
        &self,
        copy_context: &Vec<datapath_metadata_t>,
        ref_offset: usize,
        ref_length: usize,
        datapath_callback: &mut F,
        callback_state: &mut C,
    ) where
        F: FnMut(datapath_metadata_t, &mut C) -> Result<(), Fail>,
    {
        match self {
            ObjEnum::Single(single) => single.iterate_over_entries_with_callback(
                copy_context,
                ref_offset,
                ref_length,
                datapath_callback,
                callback_state,
            ),
            ObjEnum::List(list) => list.iterate_over_entries_with_callback(
                copy_context,
                ref_offset,
                ref_length,
                datapath_callback,
                callback_state,
            ),
        }
    }
}

impl Clone for ObjEnum {
    fn clone(&self) -> Self {
        match self {
            ObjEnum::Single(single) => ObjEnum::Single(single.clone()),
            ObjEnum::List(list) => ObjEnum::List(list.clone()),
        }
    }
}

impl std::fmt::Debug for ObjEnum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ObjEnum::Single(single) => single.fmt(f),
            ObjEnum::List(list) => list.fmt(f),
        }
    }
}

pub const SIZE_FIELD: usize = 4;
pub const OFFSET_FIELD: usize = 4;
/// u32 at beginning representing bitmap size in bytes
pub const BITMAP_LENGTH_FIELD: usize = 4;

#[inline]
pub fn read_size_and_offset(offset: usize, buffer: &datapath_metadata_t) -> Result<(usize, usize), Fail> {
    let forward_pointer = ForwardPointer(buffer.as_ref(), offset);
    Ok((
        forward_pointer.get_size() as usize,
        forward_pointer.get_offset() as usize,
    ))
}

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
    pub fn new(libos: &mut LibOS) -> Result<Self, Fail> {
        let buf_option = libos.allocate_tx_buffer().expect("Could not allocate tx buffer");

        match buf_option {
            Some((buf, max_len)) => {
                return Ok(SerializationCopyBuf {
                    buf,
                    total_len: max_len,
                });
            },
            None => {
                return Err(Fail::new(
                    libc::ENOMEM,
                    "Could not allocate tx buffer for serialization copying",
                ));
            },
        };
    }

    #[inline]
    pub fn to_metadata(&self) -> datapath_metadata_t {
        let len = self.len();
        self.buf.to_metadata(0, len)
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
    pub fn copy_context_ref(&self, index: usize, start: usize, len: usize, total_offset: usize) -> CopyContextRef {
        debug!("Copy context ref being made");
        let metadata_buf = self.buf.to_metadata(start, len);
        CopyContextRef::new(metadata_buf, index, start, len, total_offset)
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
    pub fn new(libos: &mut LibOS) -> Result<Self, Fail> {
        #[cfg(feature = "profiler")]
        demikernel::timer!("Allocate new copy context");
        Ok(CopyContext {
            copy_buffers: Vec::with_capacity(1),
            threshold: libos.get_copying_threshold(),
            current_length: 0,
            remaining: 0,
        })
    }

    #[inline]
    pub fn to_metadata_vec(self) -> Vec<datapath_metadata_t> {
        let vec: Vec<datapath_metadata_t> = self.copy_buffers.iter().map(|buf| buf.to_metadata()).collect();
        vec
    }

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
    pub fn push(&mut self, libos: &mut LibOS) -> Result<(), Fail> {
        let buf = SerializationCopyBuf::new(libos)?;
        self.remaining = buf.remaining();
        self.copy_buffers.push(buf);
        Ok(())
    }

    /// Copies data into copy context.
    /// Returns (start, end) range of copy context that buffer was copied into.
    #[inline]
    pub fn copy(&mut self, buf: &[u8], libos: &mut LibOS) -> Result<CopyContextRef, Fail> {
        let current_length = self.current_length;
        // TODO: doesn't work if buffer is > than an MTU
        if self.remaining < buf.len() {
            self.push(libos)?;
        }
        let copy_buffers_len = self.copy_buffers.len();
        let last_buf = &mut self.copy_buffers[copy_buffers_len - 1];
        let current_offset = last_buf.len();
        let written = last_buf.write(buf)?;
        if written != buf.len() {
            return Err(Fail::new(
                libc::EINVAL,
                &format!(
                    "Failed to write entire buf len into copy buffer, only wrote: {:?}",
                    written,
                ),
            ));
        }
        self.current_length += written;
        self.remaining -= written;
        return Ok(last_buf.copy_context_ref(copy_buffers_len - 1, current_offset, written, current_length));
    }
}
// TODO: (add doc)
pub struct CopyContextRef {
    // which buffer amongst the multiple mtu buffers
    // pointer to the index in the copy context array
    // TODO: (remove this field)
    datapath_metadata: datapath_metadata_t,
    index: usize,
    total_offset: usize,
    // might be redundant
    start: usize,
    // from data
    len: usize,
}

impl Clone for CopyContextRef {
    fn clone(&self) -> Self {
        CopyContextRef {
            datapath_metadata: self.datapath_metadata.clone(),
            index: self.index,
            start: self.start,
            len: self.len,
            total_offset: self.total_offset,
        }
    }
}

impl CopyContextRef {
    pub fn new(
        datapath_metadata: datapath_metadata_t,
        index: usize,
        start: usize,
        len: usize,
        total_offset: usize,
    ) -> Self {
        CopyContextRef {
            datapath_metadata: datapath_metadata,
            index: index,
            start: start,
            len: len,
            total_offset: total_offset,
        }
    }

    pub fn as_ref(&self) -> &[u8] {
        &self.datapath_metadata.as_ref()[self.start..(self.start + self.len)]
    }

    #[inline]
    pub fn total_offset(&self) -> usize {
        self.total_offset
    }

    #[inline]
    pub fn index(&self) -> usize {
        self.index
    }

    #[inline]
    pub fn offset(&self) -> usize {
        self.start
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }
}

/// Checks whether [seg.0, seg.0 + seg.1) is within [overarching_seg.0, overarching_seg.0 +
/// overarching_seg.1)
#[inline]
pub fn check_bounds(seg_off: usize, seg_len: usize, ref_offset: usize, ref_length: usize) -> bool {
    seg_off >= ref_offset && (seg_off + seg_len) < (ref_offset + ref_length)
}

#[inline]
pub fn sub_segment(seg_off: usize, seg_len: usize, ref_offset: usize, ref_length: usize) -> Option<(usize, usize)> {
    let start = std::cmp::min(seg_off, ref_offset);
    let end = std::cmp::max(seg_off + seg_len, ref_offset + ref_length);
    if start < end {
        return Some((start, end - start));
    } else {
        return None;
    }
}

pub trait HybridSgaHdr {
    const CONSTANT_HEADER_SIZE: usize = SIZE_FIELD + OFFSET_FIELD;
    const NUMBER_OF_FIELDS: usize = 1;
    const NUM_U32_BITMAPS: usize = 1;

    /// New 'default'
    fn new_in() -> Self
    where
        Self: Sized;

    /// Calculates number of non contiguous segments represented by this object,
    /// within the length bounds of [ref_offset, ref_offset + ref_length)
    fn num_segments_total(
        &self,
        with_header: bool,
        copy_context: &Vec<datapath_metadata_t>,
        ref_offset: usize,
        ref_length: usize,
    ) -> usize {
        let mut ret = 0;
        let mut data_offset_so_far = 0;
        let total_header_size = self.total_header_size(false);
        if with_header {
            if check_bounds(0, total_header_size, ref_offset, ref_length) {
                ret += 1;
            }
        }
        data_offset_so_far += total_header_size;

        for metadata in copy_context.iter() {
            if metadata.data_len() == 0 {
                continue;
            }
            if check_bounds(data_offset_so_far, metadata.data_len(), ref_offset, ref_length) {
                ret += 1;
            }
            data_offset_so_far += metadata.data_len();
        }

        ret += self.num_zero_copy_segments_total(ref_offset, ref_length, data_offset_so_far);
        return ret;
    }

    fn num_zero_copy_segments_total(&self, ref_offset: usize, ref_length: usize, data_offset_so_far: usize) -> usize;

    fn write_header(
        &self,
        header_buffer: &mut [u8],
        copy_context: &Vec<datapath_metadata_t>,
        ref_offset: usize,
        ref_length: usize,
    ) -> usize {
        // if the header buffer is not completely within [ref_offset, ref_offset +
        // ref_length), allocate a new buffer to write the header
        let total_header_size = self.total_header_size(false);
        let copy_context_len: usize = copy_context.iter().map(|seg| seg.data_len()).sum();

        if let Some(subseg) = sub_segment(0, total_header_size, ref_offset, ref_length) {
            let mut zc_off = 0;
            if subseg.0 != 0 || subseg.1 != total_header_size {
                let mut tmp_hdr_buf = vec![0u8; total_header_size];
                self.write_header_inner(
                    tmp_hdr_buf.as_mut_slice(),
                    total_header_size,
                    copy_context_len,
                    0,
                    self.dynamic_header_start(),
                    &mut zc_off,
                );

                // copy from tmp hdr buf to header buffer
                header_buffer[0..subseg.1].copy_from_slice(&tmp_hdr_buf.as_slice()[subseg.0..(subseg.0 + subseg.1)]);
                return subseg.1;
            } else {
                self.write_header_inner(
                    header_buffer,
                    total_header_size,
                    copy_context_len,
                    0,
                    self.dynamic_header_start(),
                    &mut zc_off,
                );
                return total_header_size;
            }
        } else {
            // don't need to write anything
            return 0;
        }
    }

    fn write_header_inner(
        &self,
        header_buffer: &mut [u8],
        header_len: usize,
        copy_context_len: usize,
        constant_header_offset: usize,
        dynamic_header_offset: usize,
        cur_zero_copy_data_off: &mut usize,
    );

    fn iterate_over_entries_with_callback<F, C>(
        &self,
        copy_context: &Vec<datapath_metadata_t>,
        ref_offset: usize,
        ref_length: usize,
        datapath_callback: &mut F,
        callback_state: &mut C,
    ) where
        F: FnMut(datapath_metadata_t, &mut C) -> Result<(), Fail>,
    {
        let header_len = self.total_header_size(false);
        let mut copy_context_len = 0;
        let mut cur_zero_copy_data_off = 0;
        for metadata in copy_context.iter() {
            if metadata.data_len() == 0 {
                continue;
            }

            if let Some(subseg) = sub_segment(
                header_len + copy_context_len,
                metadata.data_len(),
                ref_offset,
                ref_length,
            ) {
                // subset the copy context and post it
                let mut new_metadata = metadata.clone();
                let new_offset = metadata.offset() + (subseg.0 - (header_len + copy_context.len()));
                let new_len = subseg.1;
                new_metadata.set_data_len_and_offset(new_len, new_offset).unwrap();
                datapath_callback(new_metadata, callback_state).unwrap();
            }
            copy_context_len += metadata.data_len();
        }
        self.iterate_over_entries_inner(
            header_len,
            copy_context_len,
            &mut cur_zero_copy_data_off,
            datapath_callback,
            callback_state,
            ref_offset,
            ref_length,
        )
    }

    fn iterate_over_entries_inner<F, C>(
        &self,
        header_len: usize,
        copy_context_len: usize,
        cur_zero_copy_data_off: &mut usize,
        datapath_callback: &mut F,
        callback_state: &mut C,
        ref_offset: usize,
        ref_length: usize,
    ) where
        F: FnMut(datapath_metadata_t, &mut C) -> Result<(), Fail>;

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

    #[inline]
    fn is_list(&self) -> bool {
        false
    }

    /// Copies bitmap into object's bitmap, returning the space from offset that the bitmap
    /// in the serialized header format takes.
    fn deserialize_bitmap(&mut self, pkt: &datapath_metadata_t, offset: usize, buffer_offset: usize) -> usize {
        let header = pkt.as_ref();
        let bitmap_size =
            LittleEndian::read_u32(&header[(buffer_offset + offset)..(buffer_offset + offset + BITMAP_LENGTH_FIELD)]);
        self.set_bitmap(
            (0..std::cmp::min(bitmap_size, Self::NUM_U32_BITMAPS as u32) as usize).map(|i| {
                let num = LittleEndian::read_u32(
                    &header[(buffer_offset + offset + BITMAP_LENGTH_FIELD + i * 4)
                        ..(buffer_offset + offset + BITMAP_LENGTH_FIELD + (i + 1) * 4)],
                );
                Bitmap::<32>::from_value(num)
            }),
        );
        bitmap_size as usize * 4
    }

    fn serialize_bitmap(&self, header: &mut [u8], offset: usize) {
        LittleEndian::write_u32(
            &mut header[offset..(offset + BITMAP_LENGTH_FIELD)],
            Self::NUM_U32_BITMAPS as u32,
        );

        for (i, bitmap) in self.get_bitmap_iter().enumerate() {
            let slice =
                &mut header[(offset + BITMAP_LENGTH_FIELD + i * 4)..(offset + BITMAP_LENGTH_FIELD + (i + 1) * 4)];
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
    fn total_header_size(&self, with_ref: bool) -> usize {
        Self::CONSTANT_HEADER_SIZE * (with_ref as usize) + self.dynamic_header_size()
    }

    fn total_length(&self, copy_context: &CopyContext) -> usize {
        self.total_header_size(false) + copy_context.data_len() + self.zero_copy_data_len()
    }

    fn zero_copy_data_len(&self) -> usize;

    fn inner_serialize(
        &self,
        header_buffer: &mut [u8],
        constant_header_offset: usize,
        dynamic_header_offset: usize,
        copy_context: &mut CopyContext,
        zero_copy_entries: &mut [datapath_metadata_t],
        ds_offset: &mut usize,
    ) -> Result<(), Fail>;

    #[inline]
    fn serialize_into_arena_datapath_sga<'a>(
        &self,
        mut copy_context: CopyContext,
        // arena: &'a bumpalo::Bump,
    ) -> Result<DatapathSga, Fail> {
        debug!("Serializing into sga");
        let mut owned_hdr = {
            let size = self.total_header_size(false);
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
            &mut header_buffer,
            0,
            self.dynamic_header_start(),
            &mut copy_context,
            zero_copy_entries.as_mut_slice(),
            &mut ds_offset,
        )?;

        Ok(DatapathSga::new(copy_context, zero_copy_entries, owned_hdr))
    }

    fn inner_deserialize(
        &mut self,
        buf: &datapath_metadata_t,
        header_offset: usize,
        buffer_offset: usize,
    ) -> Result<(), Fail>;

    #[inline]
    fn deserialize(
        &mut self,
        pkt: &datapath_metadata_t,
        offset: usize,
        // arena: &'arena bumpalo::Bump,
    ) -> Result<(), Fail> {
        // Right now, for deserialize we assume one contiguous buffer
        // let metadata = pkt.seg(0);
        self.inner_deserialize(pkt, 0, offset)?;
        Ok(())
    }
}

pub struct DatapathSga {
    // buffers user has copied into
    _copy_context: CopyContext,
    // zero copy entries
    _zero_copy_entries: Vec<datapath_metadata_t>,
    // actual hdr
    _header: Vec<u8>,
}

impl DatapathSga {
    pub fn new(copy_context: CopyContext, zero_copy_entries: Vec<datapath_metadata_t>, header: Vec<u8>) -> Self {
        DatapathSga {
            _copy_context: copy_context,
            _zero_copy_entries: zero_copy_entries,
            _header: header,
        }
    }
}

// Basic byte array representation in Cornflakes
pub enum CFBytes {
    /// Either directly references a segment for zero-copy
    RefCounted(datapath_metadata_t),
    /// Or references the user provided copy context
    Copied(CopyContextRef),
}

impl Clone for CFBytes {
    fn clone(&self) -> Self {
        match self {
            CFBytes::RefCounted(metadata) => CFBytes::RefCounted(metadata.clone()),
            CFBytes::Copied(copy_context_ref) => CFBytes::Copied(copy_context_ref.clone()),
        }
    }
}

impl std::fmt::Debug for CFBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CFBytes::RefCounted(metadata) => f
                .debug_struct("CFBytes zero-copy")
                .field("metadata", &metadata.as_ref())
                .finish(),
            CFBytes::Copied(copy_context_ref) => f
                .debug_struct("CFBytes copied")
                .field("metadata addr", &copy_context_ref.as_ref().as_ptr())
                .field("start", &copy_context_ref.offset())
                .field("len", &copy_context_ref.len())
                .finish(),
        }
    }
}

impl CFBytes {
    pub fn new(ptr: &[u8], libos: &mut LibOS, copy_context: &mut CopyContext) -> Self {
        if copy_context.should_copy(ptr) {
            let copy_context_ref = copy_context
                .copy(ptr, libos)
                .expect("Could not copy buffers during CFBytes creation");
            return CFBytes::Copied(copy_context_ref);
        };

        match libos.recover_metadata(ptr).expect("Could not recover metadata") {
            Some(m) => CFBytes::RefCounted(m),
            None => CFBytes::Copied(
                copy_context
                    .copy(ptr, libos)
                    .expect("Could not copy buffers during CFBytes creation"),
            ),
        }
    }

    pub fn as_ref(&self) -> &[u8] {
        match self {
            CFBytes::RefCounted(m) => m.as_ref(),
            CFBytes::Copied(copy_context_ref) => copy_context_ref.as_ref(),
        }
    }

    fn default() -> Self {
        CFBytes::RefCounted(datapath_metadata_t::default())
    }
}

impl HybridSgaHdr for CFBytes {
    const CONSTANT_HEADER_SIZE: usize = SIZE_FIELD + OFFSET_FIELD;
    const NUMBER_OF_FIELDS: usize = 1;
    const NUM_U32_BITMAPS: usize = 0;

    #[inline]
    fn new_in() -> Self
    where
        Self: Sized,
    {
        CFBytes::RefCounted(datapath_metadata_t::default())
    }

    fn num_zero_copy_scatter_gather_entries(&self) -> usize {
        match self {
            CFBytes::RefCounted(_) => 1,
            CFBytes::Copied(_) => 0,
        }
    }

    #[inline]
    fn zero_copy_data_len(&self) -> usize {
        match self {
            CFBytes::RefCounted(metadata) => metadata.data_len(),
            CFBytes::Copied(_) => 0,
        }
    }

    #[inline]
    fn num_zero_copy_segments_total(&self, ref_offset: usize, ref_length: usize, data_offset_so_far: usize) -> usize {
        match self {
            CFBytes::RefCounted(metadata) => {
                // if segment is in bounds, return
                if check_bounds(data_offset_so_far, metadata.data_len(), ref_offset, ref_length) {
                    return 1;
                } else {
                    return 0;
                }
            },
            _ => {
                return 0;
            },
        }
    }

    #[inline]
    fn write_header_inner(
        &self,
        header_buffer: &mut [u8],
        header_len: usize,
        copy_context_len: usize,
        constant_header_offset: usize,
        _dynamic_header_offset: usize,
        cur_zero_copy_data_off: &mut usize,
    ) {
        match self {
            CFBytes::RefCounted(metadata) => {
                let offset_to_write = *cur_zero_copy_data_off + header_len + copy_context_len;
                let object_len = metadata.as_ref().len();
                let mut obj_ref = MutForwardPointer(header_buffer, constant_header_offset);
                obj_ref.write_size(object_len as u32);
                obj_ref.write_offset(offset_to_write as u32);
                *cur_zero_copy_data_off += object_len;
            },
            CFBytes::Copied(copy_context_ref) => {
                let offset_to_write = copy_context_ref.total_offset() + header_len;
                let mut obj_ref = MutForwardPointer(header_buffer, constant_header_offset);
                obj_ref.write_size(copy_context_ref.len() as u32);
                obj_ref.write_offset(offset_to_write as u32);
            },
        }
    }

    #[inline]
    fn iterate_over_entries_inner<F, C>(
        &self,
        header_len: usize,
        copy_context_len: usize,
        cur_zero_copy_data_off: &mut usize,
        datapath_callback: &mut F,
        callback_state: &mut C,
        ref_offset: usize,
        ref_length: usize,
    ) where
        F: FnMut(datapath_metadata_t, &mut C) -> Result<(), Fail>,
    {
        match self {
            CFBytes::RefCounted(metadata) => {
                let seg_off = header_len + copy_context_len + *cur_zero_copy_data_off;
                let seg_len = metadata.data_len();
                if let Some(subseg) = sub_segment(seg_off, seg_len, ref_offset, ref_length) {
                    let diff = subseg.0 - *cur_zero_copy_data_off;
                    let new_offset = metadata.offset() + diff;
                    let mut new_metadata = metadata.clone();
                    new_metadata.set_data_len_and_offset(subseg.1, new_offset).unwrap();
                    datapath_callback(new_metadata, callback_state).unwrap();
                }
                *cur_zero_copy_data_off += seg_len;
            },
            CFBytes::Copied(_copy_context_ref) => {
                // no need to do anything here
            },
        }
    }

    #[inline]
    fn inner_serialize(
        &self,
        header_buffer: &mut [u8],
        constant_header_offset: usize,
        _dynamic_header_start: usize,
        _copy_context: &mut CopyContext,
        zero_copy_scatter_gather_entries: &mut [datapath_metadata_t],
        ds_offset: &mut usize,
    ) -> Result<(), Fail> {
        match self {
            CFBytes::RefCounted(metadata) => {
                zero_copy_scatter_gather_entries[0] = metadata.clone();
                let offset_to_write = *ds_offset;
                let mut obj_ref = MutForwardPointer(header_buffer, constant_header_offset);
                obj_ref.write_size(metadata.as_ref().len() as u32);
                obj_ref.write_offset(offset_to_write as u32);
                *ds_offset += metadata.as_ref().len();
            },
            CFBytes::Copied(copy_context_ref) => {
                // check the copy_context against the copy context ref
                //copy_context.check(&copy_context_ref)?;
                // write in the offset and length into the correct location in the header buffer
                let offset_to_write = copy_context_ref.total_offset() + header_buffer.len();
                let mut obj_ref = MutForwardPointer(header_buffer, constant_header_offset);
                obj_ref.write_size(copy_context_ref.len() as u32);
                obj_ref.write_offset(offset_to_write as u32);
            },
        }
        Ok(())
    }

    #[inline]
    fn inner_deserialize(
        &mut self,
        buf: &datapath_metadata_t,
        header_offset: usize,
        buffer_offset: usize,
    ) -> Result<(), Fail> {
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

pub struct VariableList<T>
where
    T: HybridSgaHdr + Clone + std::fmt::Debug,
{
    num_space: usize,
    num_set: usize,
    elts: Vec<T>,
    // _phantom_data: PhantomData<D>,
}

impl<T> Clone for VariableList<T>
where
    T: HybridSgaHdr + Clone + std::fmt::Debug,
{
    fn clone(&self) -> Self {
        VariableList {
            num_space: self.num_space,
            num_set: self.num_set,
            elts: self.elts.clone(),
        }
    }
}

impl<T> std::fmt::Debug for VariableList<T>
where
    T: HybridSgaHdr + Clone + std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VariableList")
            .field("num_set", &self.num_set)
            .field("num_space", &self.num_space)
            .field("elts", &self.elts)
            .finish()
    }
}
impl<T> VariableList<T>
where
    T: HybridSgaHdr + Clone + std::fmt::Debug,
{
    #[inline]
    pub fn init(num: usize) -> VariableList<T> {
        let entries = Vec::from_iter(
            std::iter::repeat(<T>::new_in()).take(num),
            // arena,
        );
        VariableList {
            num_space: num,
            num_set: 0,
            elts: entries,
        }
    }

    #[inline]
    pub fn iter(&self) -> std::iter::Take<Iter<T>> {
        self.elts.iter().take(self.num_set)
    }

    #[inline]
    pub fn append(&mut self, val: T) {
        if self.elts.len() == self.num_set {
            self.elts.push(val);
        } else {
            self.elts[self.num_set] = val;
        }
        self.num_set += 1;
    }

    #[inline]
    pub fn replace(&mut self, idx: usize, val: T) {
        self.elts[idx] = val;
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.num_set
    }
}

impl<T> Index<usize> for VariableList<T>
where
    T: HybridSgaHdr + Clone + std::fmt::Debug,
{
    type Output = T;

    fn index(&self, idx: usize) -> &Self::Output {
        &self.elts[idx]
    }
}

impl<T> HybridSgaHdr for VariableList<T>
where
    T: HybridSgaHdr + Clone + std::fmt::Debug,
{
    const CONSTANT_HEADER_SIZE: usize = SIZE_FIELD + OFFSET_FIELD;
    const NUMBER_OF_FIELDS: usize = 1;
    const NUM_U32_BITMAPS: usize = 0;

    #[inline]
    fn new_in() -> Self
    where
        Self: Sized,
    {
        VariableList {
            num_space: 0,
            num_set: 0,
            elts: Vec::new(),
        }
    }

    #[inline]
    fn get_mut_bitmap_entry(&mut self, _offset: usize) -> &mut Bitmap<32> {
        unreachable!();
    }

    #[inline]
    fn get_bitmap_entry(&self, _offset: usize) -> &Bitmap<32> {
        unreachable!();
    }

    #[inline]
    fn dynamic_header_size(&self) -> usize {
        self.elts
            .iter()
            .map(|x| x.dynamic_header_size() + T::CONSTANT_HEADER_SIZE)
            .sum()
    }

    #[inline]
    fn dynamic_header_start(&self) -> usize {
        self.elts
            .iter()
            .take(self.num_set)
            .map(|_x| T::CONSTANT_HEADER_SIZE)
            .sum()
    }

    #[inline]
    fn num_zero_copy_scatter_gather_entries(&self) -> usize {
        self.elts
            .iter()
            .take(self.num_set)
            .map(|x| x.num_zero_copy_scatter_gather_entries())
            .sum()
    }

    #[inline]
    fn is_list(&self) -> bool {
        true
    }

    #[inline]
    fn zero_copy_data_len(&self) -> usize {
        self.elts
            .iter()
            .take(self.num_set)
            .map(|x| x.zero_copy_data_len())
            .sum()
    }

    #[inline]
    fn num_zero_copy_segments_total(
        &self,
        ref_offset: usize,
        ref_length: usize,
        mut data_offset_so_far: usize,
    ) -> usize {
        let mut ret = 0;
        for elt in self.elts.iter().take(self.num_set) {
            ret += elt.num_zero_copy_segments_total(ref_offset, ref_length, data_offset_so_far);
            data_offset_so_far += elt.zero_copy_data_len();
        }
        return ret;
    }

    #[inline]
    fn write_header_inner(
        &self,
        header_buffer: &mut [u8],
        header_len: usize,
        copy_context_len: usize,
        constant_header_offset: usize,
        dynamic_header_offset: usize,
        cur_zero_copy_data_off: &mut usize,
    ) {
        {
            let mut forward_pointer = MutForwardPointer(header_buffer, constant_header_offset);
            forward_pointer.write_size(self.num_set as u32);
            forward_pointer.write_offset(dynamic_header_offset as u32);
        }

        let mut cur_dynamic_off = dynamic_header_offset + self.dynamic_header_start();
        for (i, elt) in self.elts.iter().take(self.num_set).enumerate() {
            if elt.dynamic_header_size() != 0 {
                let mut forward_offset =
                    MutForwardPointer(header_buffer, dynamic_header_offset + T::CONSTANT_HEADER_SIZE * i);
                // TODO: might be unnecessary
                forward_offset.write_size(elt.dynamic_header_size() as u32);
                forward_offset.write_offset(cur_dynamic_off as u32);
                elt.write_header_inner(
                    header_buffer,
                    header_len,
                    copy_context_len,
                    cur_dynamic_off,
                    cur_dynamic_off + elt.dynamic_header_start(),
                    cur_zero_copy_data_off,
                );
            } else {
                elt.write_header_inner(
                    header_buffer,
                    header_len,
                    copy_context_len,
                    dynamic_header_offset + T::CONSTANT_HEADER_SIZE * i,
                    cur_dynamic_off,
                    cur_zero_copy_data_off,
                );
            }
            cur_dynamic_off += elt.dynamic_header_size();
        }
    }

    #[inline]
    fn iterate_over_entries_inner<F, C>(
        &self,
        header_len: usize,
        copy_context_len: usize,
        cur_zero_copy_data_off: &mut usize,
        datapath_callback: &mut F,
        callback_state: &mut C,
        ref_offset: usize,
        ref_length: usize,
    ) where
        F: FnMut(datapath_metadata_t, &mut C) -> Result<(), Fail>,
    {
        for elt in self.elts.iter().take(self.num_set) {
            elt.iterate_over_entries_inner(
                header_len,
                copy_context_len,
                cur_zero_copy_data_off,
                datapath_callback,
                callback_state,
                ref_offset,
                ref_length,
            );
        }
    }

    #[inline]
    fn inner_serialize(
        &self,
        header_buffer: &mut [u8],
        constant_header_offset: usize,
        dynamic_header_start: usize,
        copy_context: &mut CopyContext,
        zero_copy_scatter_gather_entries: &mut [datapath_metadata_t],
        ds_offset: &mut usize,
    ) -> Result<(), Fail> {
        {
            let mut forward_pointer = MutForwardPointer(header_buffer, constant_header_offset);
            forward_pointer.write_size(self.num_set as u32);
            forward_pointer.write_offset(dynamic_header_start as u32);
        }
        let mut sge_idx = 0;
        let mut cur_dynamic_off = dynamic_header_start + self.dynamic_header_start();
        for (i, elt) in self.elts.iter().take(self.num_set).enumerate() {
            let required_sges = elt.num_zero_copy_scatter_gather_entries();
            if elt.dynamic_header_size() != 0 {
                let mut forward_offset =
                    MutForwardPointer(header_buffer, dynamic_header_start + T::CONSTANT_HEADER_SIZE * i);
                // TODO: might be unnecessary
                forward_offset.write_size(elt.dynamic_header_size() as u32);
                forward_offset.write_offset(cur_dynamic_off as u32);
                elt.inner_serialize(
                    header_buffer,
                    cur_dynamic_off,
                    cur_dynamic_off + elt.dynamic_header_start(),
                    copy_context,
                    &mut zero_copy_scatter_gather_entries[sge_idx..(sge_idx + required_sges)],
                    ds_offset,
                )?;
            } else {
                elt.inner_serialize(
                    header_buffer,
                    dynamic_header_start + T::CONSTANT_HEADER_SIZE * i,
                    cur_dynamic_off,
                    copy_context,
                    &mut zero_copy_scatter_gather_entries[sge_idx..(sge_idx + required_sges)],
                    ds_offset,
                )?;
            }
            sge_idx += required_sges;
            cur_dynamic_off += elt.dynamic_header_size();
        }
        Ok(())
    }

    #[inline]
    fn inner_deserialize(
        &mut self,
        buffer: &datapath_metadata_t,
        constant_offset: usize,
        buffer_offset: usize,
        // arena: &'arena bumpalo::Bump,
    ) -> Result<(), Fail> {
        let forward_pointer = ForwardPointer(buffer.as_ref(), constant_offset + buffer_offset);
        let size = forward_pointer.get_size() as usize;
        let dynamic_offset = forward_pointer.get_offset() as usize;

        self.num_set = size;
        //self.elts = bumpalo::vec![in &arena; T::new_in(arena); size];
        if self.elts.len() < size {
            self.elts.resize(size, T::new_in());
        }
        self.num_space = size;

        for (i, elt) in self.elts.iter_mut().take(size).enumerate() {
            if elt.dynamic_header_size() == 0 {
                elt.inner_deserialize(buffer, dynamic_offset + i * T::CONSTANT_HEADER_SIZE, buffer_offset)?;
            } else {
                let (_size, dynamic_off) = read_size_and_offset(dynamic_offset + i * T::CONSTANT_HEADER_SIZE, buffer)?;
                elt.inner_deserialize(buffer, dynamic_off, buffer_offset)?;
            }
        }
        Ok(())
    }
}
