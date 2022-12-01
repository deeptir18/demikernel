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
use crate::cornflakes::{
    CFBytes,
    HybridSgaHdr,
    VariableList,
    SIZE_FIELD,
    OFFSET_FIELD,
    BITMAP_LENGTH_FIELD,
    CallbackEntryState,
    CopyContext,
};

use std::{default::Default, marker::PhantomData, marker::Sized, ops::Index, slice::Iter, str};
use bitmaps::Bitmap;
use byteorder::{ByteOrder, LittleEndian};
use anyhow::{
    bail,
    Error,
};

pub const SingleBufferCF_NUM_U32_BITMAPS: usize = 1;

pub struct SingleBufferCF<'registered> {
    bitmap: [Bitmap<32>; SingleBufferCF_NUM_U32_BITMAPS],
    message: CFBytes<'registered>,
}

impl<'registered> Clone for SingleBufferCF<'registered>{
    #[inline]
    fn clone(&self) -> Self {
        SingleBufferCF {
            bitmap: self.bitmap.clone(),
            message: self.message.clone(),
        }
    }
}

impl<'registered> std::fmt::Debug for SingleBufferCF<'registered>{
    
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SingleBufferCF")
            .field("message", &self.message)
            .finish()
    }
}

impl<'registered> SingleBufferCF<'registered> {
    const MESSAGE_BITMAP_IDX: usize = 0;
    const MESSAGE_BITMAP_OFFSET: usize = 0;

    #[inline]
    pub fn has_message(&self) -> bool {
        self.bitmap[Self::MESSAGE_BITMAP_OFFSET].get(Self::MESSAGE_BITMAP_IDX)
    }

    #[inline]
    pub fn get_message(&self) -> &CFBytes<'registered> {
        &self.message
    }

    #[inline]
    pub fn set_message(&mut self, field: CFBytes<'registered>) {
        self.bitmap[Self::MESSAGE_BITMAP_OFFSET].set(Self::MESSAGE_BITMAP_IDX, true);
        self.message = field;
    }
}

impl<'registered> HybridSgaHdr for SingleBufferCF<'registered> {
    const NUMBER_OF_FIELDS: usize = 1;

    const CONSTANT_HEADER_SIZE: usize = SIZE_FIELD + OFFSET_FIELD;

    const NUM_U32_BITMAPS: usize = SingleBufferCF_NUM_U32_BITMAPS;
    #[inline]
    fn new_in() -> Self
    where
        Self: Sized,
    {
        SingleBufferCF {
            bitmap: [Bitmap::<32>::new(); SingleBufferCF_NUM_U32_BITMAPS],
            message: CFBytes::default(),
        }
    }

    #[inline]
    fn dynamic_header_size(&self) -> usize {
        BITMAP_LENGTH_FIELD
            + Self::bitmap_length()
            + self.get_bitmap_field(Self::MESSAGE_BITMAP_IDX, Self::MESSAGE_BITMAP_OFFSET) as usize
                * self.message.total_header_size(true, false)
    }

    #[inline]
    fn dynamic_header_start(&self) -> usize {
        BITMAP_LENGTH_FIELD
            + Self::bitmap_length()
            + self.get_bitmap_field(Self::MESSAGE_BITMAP_IDX, Self::MESSAGE_BITMAP_OFFSET) as usize
                * CFBytes::CONSTANT_HEADER_SIZE
    }

    #[inline]
    fn num_zero_copy_scatter_gather_entries(&self) -> usize {
        0 + self.message.num_zero_copy_scatter_gather_entries()
    }

    #[inline]
    fn get_bitmap_itermut(&mut self) -> std::slice::IterMut<Bitmap<32>> {
        self.bitmap.iter_mut()
    }

    #[inline]
    fn get_bitmap_iter(&self) -> std::slice::Iter<Bitmap<32>> {
        self.bitmap.iter()
    }

    #[inline]
    fn get_mut_bitmap_entry(&mut self, offset: usize) -> &mut Bitmap<32> {
        &mut self.bitmap[offset]
    }

    #[inline]
    fn get_bitmap_entry(&self, offset: usize) -> &Bitmap<32> {
        &self.bitmap[offset]
    }

    #[inline]
    fn set_bitmap(&mut self, bitmap: impl Iterator<Item = Bitmap<32>>) {
        for (bitmap_entry, bits) in self.bitmap.iter_mut().zip(bitmap) {
            *bitmap_entry = bits;
        }
    }

    // #[inline]
    // fn check_deep_equality(&self, other: &Self) -> bool {
    //     if self.get_bitmap_field(Self::MESSAGE_BITMAP_IDX, Self::MESSAGE_BITMAP_OFFSET)
    //         != other.get_bitmap_field(Self::MESSAGE_BITMAP_IDX, Self::MESSAGE_BITMAP_OFFSET)
    //     {
    //         return false;
    //     } else if self.get_bitmap_field(Self::MESSAGE_BITMAP_IDX, Self::MESSAGE_BITMAP_OFFSET)
    //         && other.get_bitmap_field(Self::MESSAGE_BITMAP_IDX, Self::MESSAGE_BITMAP_OFFSET)
    //     {
    //         if !self.get_message().check_deep_equality(&other.get_message()) {
    //             return false;
    //         }
    //     }

    //     return true;
    // }

    #[inline]
    fn iterate_over_entries<F>(
        &self,
        copy_context: &mut CopyContext,
        header_len: usize,
        header_buffer: &mut [u8],
        constant_header_offset: usize,
        dynamic_header_offset: usize,
        cur_entry_ptr: &mut usize,
        datapath_callback: &mut F,
        callback_state: &mut CallbackEntryState,
    ) -> Result<usize, Error>
    where
        F: FnMut(&datapath_metadata_t, &mut CallbackEntryState) -> Result<(), Error>,
    {
        self.serialize_bitmap(header_buffer, constant_header_offset);
        let cur_constant_offset =
            constant_header_offset + BITMAP_LENGTH_FIELD + Self::bitmap_length();

        let cur_dynamic_offset = dynamic_header_offset;
        let mut ret = 0;

        if self.get_bitmap_field(Self::MESSAGE_BITMAP_IDX, Self::MESSAGE_BITMAP_OFFSET) {
            ret += self.message.iterate_over_entries(
                copy_context,
                header_len,
                header_buffer,
                cur_constant_offset,
                cur_dynamic_offset,
                cur_entry_ptr,
                datapath_callback,
                callback_state,
            )?;
        }

        Ok(ret)
    }

    #[inline]
    fn inner_serialize<'a>(
        &self,
        datapath: &mut LibOS,
        header_buffer: &mut [u8],
        constant_header_offset: usize,
        dynamic_header_start: usize,
        copy_context: &mut CopyContext,
        zero_copy_entries: &mut [datapath_metadata_t],
        ds_offset: &mut usize,
    ) -> Result<(), Error> {
        self.serialize_bitmap(header_buffer, constant_header_offset);
        let cur_constant_offset =
            constant_header_offset + BITMAP_LENGTH_FIELD + Self::bitmap_length();

        let cur_dynamic_offset = dynamic_header_start;
        let cur_sge_idx = 0;

        if self.get_bitmap_field(Self::MESSAGE_BITMAP_IDX, Self::MESSAGE_BITMAP_OFFSET) {
            self.message.inner_serialize(
                datapath,
                header_buffer,
                cur_constant_offset,
                cur_dynamic_offset,
                copy_context,
                &mut zero_copy_entries[cur_sge_idx
                    ..(cur_sge_idx + self.message.num_zero_copy_scatter_gather_entries())],
                ds_offset,
            )?;
        }

        Ok(())
    }

    #[inline]
    fn inner_deserialize(
        &mut self,
        buf: &datapath_metadata_t,
        header_offset: usize,
        buffer_offset: usize,
        // arena: &'arena bumpalo::Bump,
    ) -> Result<(), Error> {
        let bitmap_size = self.deserialize_bitmap(buf, header_offset, buffer_offset);
        let cur_constant_offset = header_offset + BITMAP_LENGTH_FIELD + bitmap_size;

        if self.get_bitmap_field(Self::MESSAGE_BITMAP_IDX, Self::MESSAGE_BITMAP_OFFSET) {
            self.message
                .inner_deserialize(buf, cur_constant_offset, buffer_offset)?;
        }

        Ok(())
    }
}

pub const ListCF_NUM_U32_BITMAPS: usize = 1;

pub struct ListCF<'registered> {
    bitmap: [Bitmap<32>; ListCF_NUM_U32_BITMAPS],
    messages: VariableList<CFBytes<'registered>>,
}

// impl<'registered> Clone for ListCF<'registered>
// where
//     D: Datapath,
// {
//     #[inline]
//     fn clone(&self) -> Self {
//         ListCF {
//             bitmap: self.bitmap.clone(),
//             messages: self.messages.clone(),
//         }
//     }
// }

// impl<'arena, 'registered, D> std::fmt::Debug for ListCF<'arena, 'registered, D>
// where
//     D: Datapath,
// {
//     #[inline]
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         f.debug_struct("ListCF")
//             .field("messages", &self.messages)
//             .finish()
//     }
// }

// impl<'arena, 'registered, D> ListCF<'arena, 'registered, D>
// where
//     D: Datapath,
// {
//     const MESSAGES_BITMAP_IDX: usize = 0;
//     const MESSAGES_BITMAP_OFFSET: usize = 0;

//     #[inline]
//     pub fn has_messages(&self) -> bool {
//         self.bitmap[Self::MESSAGES_BITMAP_OFFSET].get(Self::MESSAGES_BITMAP_IDX)
//     }

//     #[inline]
//     pub fn get_messages(&self) -> &VariableList<'arena, CFBytes<'registered, D>, D> {
//         &self.messages
//     }

//     #[inline]
//     pub fn set_messages(&mut self, field: VariableList<'arena, CFBytes<'registered, D>, D>) {
//         self.bitmap[Self::MESSAGES_BITMAP_OFFSET].set(Self::MESSAGES_BITMAP_IDX, true);
//         self.messages = field;
//     }

//     #[inline]
//     pub fn get_mut_messages(&mut self) -> &mut VariableList<'arena, CFBytes<'registered, D>, D> {
//         &mut self.messages
//     }
//     #[inline]
//     pub fn init_messages(&mut self, num: usize, arena: &'arena bumpalo::Bump) {
//         self.messages = VariableList::init(num, arena);
//         self.set_bitmap_field(Self::MESSAGES_BITMAP_IDX, Self::MESSAGES_BITMAP_OFFSET);
//     }
// }

// impl<'arena, 'registered, D> HybridArenaRcSgaHdr<'arena, D> for ListCF<'arena, 'registered, D>
// where
//     D: Datapath,
// {
//     const NUMBER_OF_FIELDS: usize = 1;

//     const CONSTANT_HEADER_SIZE: usize = SIZE_FIELD + OFFSET_FIELD;

//     const NUM_U32_BITMAPS: usize = ListCF_NUM_U32_BITMAPS;
//     #[inline]
//     fn new_in(arena: &'arena bumpalo::Bump) -> Self
//     where
//         Self: Sized,
//     {
//         ListCF {
//             bitmap: [Bitmap::<32>::new(); ListCF_NUM_U32_BITMAPS],
//             messages: VariableList::new_in(arena),
//         }
//     }

//     #[inline]
//     fn dynamic_header_size(&self) -> usize {
//         BITMAP_LENGTH_FIELD
//             + Self::bitmap_length()
//             + self.get_bitmap_field(Self::MESSAGES_BITMAP_IDX, Self::MESSAGES_BITMAP_OFFSET)
//                 as usize
//                 * self.messages.total_header_size(true, false)
//     }

//     #[inline]
//     fn dynamic_header_start(&self) -> usize {
//         BITMAP_LENGTH_FIELD
//             + Self::bitmap_length()
//             + self.get_bitmap_field(Self::MESSAGES_BITMAP_IDX, Self::MESSAGES_BITMAP_OFFSET)
//                 as usize
//                 * VariableList::<CFBytes<D>, D>::CONSTANT_HEADER_SIZE
//     }

//     #[inline]
//     fn num_zero_copy_scatter_gather_entries(&self) -> usize {
//         0 + self.messages.num_zero_copy_scatter_gather_entries()
//     }

//     #[inline]
//     fn get_bitmap_itermut(&mut self) -> std::slice::IterMut<Bitmap<32>> {
//         self.bitmap.iter_mut()
//     }

//     #[inline]
//     fn get_bitmap_iter(&self) -> std::slice::Iter<Bitmap<32>> {
//         self.bitmap.iter()
//     }

//     #[inline]
//     fn get_mut_bitmap_entry(&mut self, offset: usize) -> &mut Bitmap<32> {
//         &mut self.bitmap[offset]
//     }

//     #[inline]
//     fn get_bitmap_entry(&self, offset: usize) -> &Bitmap<32> {
//         &self.bitmap[offset]
//     }

//     #[inline]
//     fn set_bitmap(&mut self, bitmap: impl Iterator<Item = Bitmap<32>>) {
//         for (bitmap_entry, bits) in self.bitmap.iter_mut().zip(bitmap) {
//             *bitmap_entry = bits;
//         }
//     }

//     #[inline]
//     fn check_deep_equality(&self, other: &Self) -> bool {
//         if self.get_bitmap_field(Self::MESSAGES_BITMAP_IDX, Self::MESSAGES_BITMAP_OFFSET)
//             != other.get_bitmap_field(Self::MESSAGES_BITMAP_IDX, Self::MESSAGES_BITMAP_OFFSET)
//         {
//             return false;
//         } else if self.get_bitmap_field(Self::MESSAGES_BITMAP_IDX, Self::MESSAGES_BITMAP_OFFSET)
//             && other.get_bitmap_field(Self::MESSAGES_BITMAP_IDX, Self::MESSAGES_BITMAP_OFFSET)
//         {
//             if !self
//                 .get_messages()
//                 .check_deep_equality(&other.get_messages())
//             {
//                 return false;
//             }
//         }

//         return true;
//     }

//     #[inline]
//     fn iterate_over_entries<F>(
//         &self,
//         copy_context: &mut CopyContext<'arena, D>,
//         header_len: usize,
//         header_buffer: &mut [u8],
//         constant_header_offset: usize,
//         dynamic_header_offset: usize,
//         cur_entry_ptr: &mut usize,
//         datapath_callback: &mut F,
//         callback_state: &mut D::CallbackEntryState,
//     ) -> Result<usize>
//     where
//         F: FnMut(&D::DatapathMetadata, &mut D::CallbackEntryState) -> Result<()>,
//     {
//         self.serialize_bitmap(header_buffer, constant_header_offset);
//         let cur_constant_offset =
//             constant_header_offset + BITMAP_LENGTH_FIELD + Self::bitmap_length();

//         let cur_dynamic_offset = dynamic_header_offset;
//         let mut ret = 0;

//         if self.get_bitmap_field(Self::MESSAGES_BITMAP_IDX, Self::MESSAGES_BITMAP_OFFSET) {
//             ret += self.messages.iterate_over_entries(
//                 copy_context,
//                 header_len,
//                 header_buffer,
//                 cur_constant_offset,
//                 cur_dynamic_offset,
//                 cur_entry_ptr,
//                 datapath_callback,
//                 callback_state,
//             )?;
//         }

//         Ok(ret)
//     }

//     #[inline]
//     fn inner_serialize<'a>(
//         &self,
//         datapath: &mut D,
//         header: &mut [u8],
//         constant_header_offset: usize,
//         dynamic_header_start: usize,
//         copy_context: &mut CopyContext<'a, D>,
//         zero_copy_scatter_gather_entries: &mut [D::DatapathMetadata],
//         ds_offset: &mut usize,
//     ) -> Result<()> {
//         self.serialize_bitmap(header, constant_header_offset);
//         let cur_constant_offset =
//             constant_header_offset + BITMAP_LENGTH_FIELD + Self::bitmap_length();

//         let cur_dynamic_offset = dynamic_header_start;
//         let cur_sge_idx = 0;

//         if self.get_bitmap_field(Self::MESSAGES_BITMAP_IDX, Self::MESSAGES_BITMAP_OFFSET) {
//             self.messages.inner_serialize(
//                 datapath,
//                 header,
//                 cur_constant_offset,
//                 cur_dynamic_offset,
//                 copy_context,
//                 &mut zero_copy_scatter_gather_entries[cur_sge_idx
//                     ..(cur_sge_idx + self.messages.num_zero_copy_scatter_gather_entries())],
//                 ds_offset,
//             )?;
//         }

//         Ok(())
//     }

//     #[inline]
//     fn inner_deserialize(
//         &mut self,
//         buffer: &D::DatapathMetadata,
//         header_offset: usize,
//         buffer_offset: usize,
//         arena: &'arena bumpalo::Bump,
//     ) -> Result<()> {
//         let bitmap_size = self.deserialize_bitmap(buffer, header_offset, buffer_offset);
//         let cur_constant_offset = header_offset + BITMAP_LENGTH_FIELD + bitmap_size;

//         if self.get_bitmap_field(Self::MESSAGES_BITMAP_IDX, Self::MESSAGES_BITMAP_OFFSET) {
//             self.messages
//                 .inner_deserialize(buffer, cur_constant_offset, buffer_offset, arena)?;
//         }

//         Ok(())
//     }
// }
