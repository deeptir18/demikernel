use bitmaps::Bitmap;
use color_eyre::eyre::Result;
// use cornflakes_libos::dynamic_rcsga_hybrid_hdr::HybridArenaRcSgaHdr;
// use cornflakes_libos::dynamic_rcsga_hybrid_hdr::*;
// use cornflakes_libos::{datapath::Datapath, CopyContext};

pub const SingleBufferCF_NUM_U32_BITMAPS: usize = 1;

pub struct SingleBufferCF<'registered> {
    bitmap: [Bitmap<32>; SingleBufferCF_NUM_U32_BITMAPS],
    message: CFBytes<'registered>,
}

impl<'registered, D> Clone for SingleBufferCF<'registered, D>
where
    D: Datapath,
{
    #[inline]
    fn clone(&self) -> Self {
        SingleBufferCF {
            bitmap: self.bitmap.clone(),
            message: self.message.clone(),
        }
    }
}

impl<'registered, D> std::fmt::Debug for SingleBufferCF<'registered, D>
where
    D: Datapath,
{
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SingleBufferCF")
            .field("message", &self.message)
            .finish()
    }
}

impl<'registered, D> SingleBufferCF<'registered, D>
where
    D: Datapath,
{
    const MESSAGE_BITMAP_IDX: usize = 0;
    const MESSAGE_BITMAP_OFFSET: usize = 0;

    #[inline]
    pub fn has_message(&self) -> bool {
        self.bitmap[Self::MESSAGE_BITMAP_OFFSET].get(Self::MESSAGE_BITMAP_IDX)
    }

    #[inline]
    pub fn get_message(&self) -> &CFBytes<'registered, D> {
        &self.message
    }

    #[inline]
    pub fn set_message(&mut self, field: CFBytes<'registered, D>) {
        self.bitmap[Self::MESSAGE_BITMAP_OFFSET].set(Self::MESSAGE_BITMAP_IDX, true);
        self.message = field;
    }
}

impl<'arena, 'registered, D> HybridArenaRcSgaHdr<'arena, D> for SingleBufferCF<'registered, D>
where
    D: Datapath,
{
    const NUMBER_OF_FIELDS: usize = 1;

    const CONSTANT_HEADER_SIZE: usize = SIZE_FIELD + OFFSET_FIELD;

    const NUM_U32_BITMAPS: usize = SingleBufferCF_NUM_U32_BITMAPS;
    #[inline]
    fn new_in(arena: &'arena bumpalo::Bump) -> Self
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
                * CFBytes::<D>::CONSTANT_HEADER_SIZE
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

    #[inline]
    fn check_deep_equality(&self, other: &Self) -> bool {
        if self.get_bitmap_field(Self::MESSAGE_BITMAP_IDX, Self::MESSAGE_BITMAP_OFFSET)
            != other.get_bitmap_field(Self::MESSAGE_BITMAP_IDX, Self::MESSAGE_BITMAP_OFFSET)
        {
            return false;
        } else if self.get_bitmap_field(Self::MESSAGE_BITMAP_IDX, Self::MESSAGE_BITMAP_OFFSET)
            && other.get_bitmap_field(Self::MESSAGE_BITMAP_IDX, Self::MESSAGE_BITMAP_OFFSET)
        {
            if !self.get_message().check_deep_equality(&other.get_message()) {
                return false;
            }
        }

        return true;
    }

    #[inline]
    fn iterate_over_entries<F>(
        &self,
        copy_context: &mut CopyContext<'arena, D>,
        header_len: usize,
        header_buffer: &mut [u8],
        constant_header_offset: usize,
        dynamic_header_offset: usize,
        cur_entry_ptr: &mut usize,
        datapath_callback: &mut F,
        callback_state: &mut D::CallbackEntryState,
    ) -> Result<usize>
    where
        F: FnMut(&D::DatapathMetadata, &mut D::CallbackEntryState) -> Result<()>,
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
        datapath: &mut D,
        header: &mut [u8],
        constant_header_offset: usize,
        dynamic_header_start: usize,
        copy_context: &mut CopyContext<'a, D>,
        zero_copy_scatter_gather_entries: &mut [D::DatapathMetadata],
        ds_offset: &mut usize,
    ) -> Result<()> {
        self.serialize_bitmap(header, constant_header_offset);
        let cur_constant_offset =
            constant_header_offset + BITMAP_LENGTH_FIELD + Self::bitmap_length();

        let cur_dynamic_offset = dynamic_header_start;
        let cur_sge_idx = 0;

        if self.get_bitmap_field(Self::MESSAGE_BITMAP_IDX, Self::MESSAGE_BITMAP_OFFSET) {
            self.message.inner_serialize(
                datapath,
                header,
                cur_constant_offset,
                cur_dynamic_offset,
                copy_context,
                &mut zero_copy_scatter_gather_entries[cur_sge_idx
                    ..(cur_sge_idx + self.message.num_zero_copy_scatter_gather_entries())],
                ds_offset,
            )?;
        }

        Ok(())
    }

    #[inline]
    fn inner_deserialize(
        &mut self,
        buffer: &D::DatapathMetadata,
        header_offset: usize,
        buffer_offset: usize,
        arena: &'arena bumpalo::Bump,
    ) -> Result<()> {
        let bitmap_size = self.deserialize_bitmap(buffer, header_offset, buffer_offset);
        let cur_constant_offset = header_offset + BITMAP_LENGTH_FIELD + bitmap_size;

        if self.get_bitmap_field(Self::MESSAGE_BITMAP_IDX, Self::MESSAGE_BITMAP_OFFSET) {
            self.message
                .inner_deserialize(buffer, cur_constant_offset, buffer_offset, arena)?;
        }

        Ok(())
    }
}

pub const ListCF_NUM_U32_BITMAPS: usize = 1;

pub struct ListCF<'arena, 'registered, D>
where
    D: Datapath,
{
    bitmap: [Bitmap<32>; ListCF_NUM_U32_BITMAPS],
    messages: VariableList<'arena, CFBytes<'registered, D>, D>,
}

impl<'arena, 'registered, D> Clone for ListCF<'arena, 'registered, D>
where
    D: Datapath,
{
    #[inline]
    fn clone(&self) -> Self {
        ListCF {
            bitmap: self.bitmap.clone(),
            messages: self.messages.clone(),
        }
    }
}

impl<'arena, 'registered, D> std::fmt::Debug for ListCF<'arena, 'registered, D>
where
    D: Datapath,
{
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ListCF")
            .field("messages", &self.messages)
            .finish()
    }
}

impl<'arena, 'registered, D> ListCF<'arena, 'registered, D>
where
    D: Datapath,
{
    const MESSAGES_BITMAP_IDX: usize = 0;
    const MESSAGES_BITMAP_OFFSET: usize = 0;

    #[inline]
    pub fn has_messages(&self) -> bool {
        self.bitmap[Self::MESSAGES_BITMAP_OFFSET].get(Self::MESSAGES_BITMAP_IDX)
    }

    #[inline]
    pub fn get_messages(&self) -> &VariableList<'arena, CFBytes<'registered, D>, D> {
        &self.messages
    }

    #[inline]
    pub fn set_messages(&mut self, field: VariableList<'arena, CFBytes<'registered, D>, D>) {
        self.bitmap[Self::MESSAGES_BITMAP_OFFSET].set(Self::MESSAGES_BITMAP_IDX, true);
        self.messages = field;
    }

    #[inline]
    pub fn get_mut_messages(&mut self) -> &mut VariableList<'arena, CFBytes<'registered, D>, D> {
        &mut self.messages
    }
    #[inline]
    pub fn init_messages(&mut self, num: usize, arena: &'arena bumpalo::Bump) {
        self.messages = VariableList::init(num, arena);
        self.set_bitmap_field(Self::MESSAGES_BITMAP_IDX, Self::MESSAGES_BITMAP_OFFSET);
    }
}

impl<'arena, 'registered, D> HybridArenaRcSgaHdr<'arena, D> for ListCF<'arena, 'registered, D>
where
    D: Datapath,
{
    const NUMBER_OF_FIELDS: usize = 1;

    const CONSTANT_HEADER_SIZE: usize = SIZE_FIELD + OFFSET_FIELD;

    const NUM_U32_BITMAPS: usize = ListCF_NUM_U32_BITMAPS;
    #[inline]
    fn new_in(arena: &'arena bumpalo::Bump) -> Self
    where
        Self: Sized,
    {
        ListCF {
            bitmap: [Bitmap::<32>::new(); ListCF_NUM_U32_BITMAPS],
            messages: VariableList::new_in(arena),
        }
    }

    #[inline]
    fn dynamic_header_size(&self) -> usize {
        BITMAP_LENGTH_FIELD
            + Self::bitmap_length()
            + self.get_bitmap_field(Self::MESSAGES_BITMAP_IDX, Self::MESSAGES_BITMAP_OFFSET)
                as usize
                * self.messages.total_header_size(true, false)
    }

    #[inline]
    fn dynamic_header_start(&self) -> usize {
        BITMAP_LENGTH_FIELD
            + Self::bitmap_length()
            + self.get_bitmap_field(Self::MESSAGES_BITMAP_IDX, Self::MESSAGES_BITMAP_OFFSET)
                as usize
                * VariableList::<CFBytes<D>, D>::CONSTANT_HEADER_SIZE
    }

    #[inline]
    fn num_zero_copy_scatter_gather_entries(&self) -> usize {
        0 + self.messages.num_zero_copy_scatter_gather_entries()
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

    #[inline]
    fn check_deep_equality(&self, other: &Self) -> bool {
        if self.get_bitmap_field(Self::MESSAGES_BITMAP_IDX, Self::MESSAGES_BITMAP_OFFSET)
            != other.get_bitmap_field(Self::MESSAGES_BITMAP_IDX, Self::MESSAGES_BITMAP_OFFSET)
        {
            return false;
        } else if self.get_bitmap_field(Self::MESSAGES_BITMAP_IDX, Self::MESSAGES_BITMAP_OFFSET)
            && other.get_bitmap_field(Self::MESSAGES_BITMAP_IDX, Self::MESSAGES_BITMAP_OFFSET)
        {
            if !self
                .get_messages()
                .check_deep_equality(&other.get_messages())
            {
                return false;
            }
        }

        return true;
    }

    #[inline]
    fn iterate_over_entries<F>(
        &self,
        copy_context: &mut CopyContext<'arena, D>,
        header_len: usize,
        header_buffer: &mut [u8],
        constant_header_offset: usize,
        dynamic_header_offset: usize,
        cur_entry_ptr: &mut usize,
        datapath_callback: &mut F,
        callback_state: &mut D::CallbackEntryState,
    ) -> Result<usize>
    where
        F: FnMut(&D::DatapathMetadata, &mut D::CallbackEntryState) -> Result<()>,
    {
        self.serialize_bitmap(header_buffer, constant_header_offset);
        let cur_constant_offset =
            constant_header_offset + BITMAP_LENGTH_FIELD + Self::bitmap_length();

        let cur_dynamic_offset = dynamic_header_offset;
        let mut ret = 0;

        if self.get_bitmap_field(Self::MESSAGES_BITMAP_IDX, Self::MESSAGES_BITMAP_OFFSET) {
            ret += self.messages.iterate_over_entries(
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
        datapath: &mut D,
        header: &mut [u8],
        constant_header_offset: usize,
        dynamic_header_start: usize,
        copy_context: &mut CopyContext<'a, D>,
        zero_copy_scatter_gather_entries: &mut [D::DatapathMetadata],
        ds_offset: &mut usize,
    ) -> Result<()> {
        self.serialize_bitmap(header, constant_header_offset);
        let cur_constant_offset =
            constant_header_offset + BITMAP_LENGTH_FIELD + Self::bitmap_length();

        let cur_dynamic_offset = dynamic_header_start;
        let cur_sge_idx = 0;

        if self.get_bitmap_field(Self::MESSAGES_BITMAP_IDX, Self::MESSAGES_BITMAP_OFFSET) {
            self.messages.inner_serialize(
                datapath,
                header,
                cur_constant_offset,
                cur_dynamic_offset,
                copy_context,
                &mut zero_copy_scatter_gather_entries[cur_sge_idx
                    ..(cur_sge_idx + self.messages.num_zero_copy_scatter_gather_entries())],
                ds_offset,
            )?;
        }

        Ok(())
    }

    #[inline]
    fn inner_deserialize(
        &mut self,
        buffer: &D::DatapathMetadata,
        header_offset: usize,
        buffer_offset: usize,
        arena: &'arena bumpalo::Bump,
    ) -> Result<()> {
        let bitmap_size = self.deserialize_bitmap(buffer, header_offset, buffer_offset);
        let cur_constant_offset = header_offset + BITMAP_LENGTH_FIELD + bitmap_size;

        if self.get_bitmap_field(Self::MESSAGES_BITMAP_IDX, Self::MESSAGES_BITMAP_OFFSET) {
            self.messages
                .inner_deserialize(buffer, cur_constant_offset, buffer_offset, arena)?;
        }

        Ok(())
    }
}

pub const Tree1LCF_NUM_U32_BITMAPS: usize = 1;

pub struct Tree1LCF<'registered, D>
where
    D: Datapath,
{
    bitmap: [Bitmap<32>; Tree1LCF_NUM_U32_BITMAPS],
    left: SingleBufferCF<'registered, D>,
    right: SingleBufferCF<'registered, D>,
}

impl<'registered, D> Clone for Tree1LCF<'registered, D>
where
    D: Datapath,
{
    #[inline]
    fn clone(&self) -> Self {
        Tree1LCF {
            bitmap: self.bitmap.clone(),
            left: self.left.clone(),
            right: self.right.clone(),
        }
    }
}

impl<'registered, D> std::fmt::Debug for Tree1LCF<'registered, D>
where
    D: Datapath,
{
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tree1LCF")
            .field("left", &self.left)
            .field("right", &self.right)
            .finish()
    }
}

impl<'registered, D> Tree1LCF<'registered, D>
where
    D: Datapath,
{
    const LEFT_BITMAP_IDX: usize = 0;
    const LEFT_BITMAP_OFFSET: usize = 0;

    const RIGHT_BITMAP_IDX: usize = 1;
    const RIGHT_BITMAP_OFFSET: usize = 0;

    #[inline]
    pub fn has_left(&self) -> bool {
        self.bitmap[Self::LEFT_BITMAP_OFFSET].get(Self::LEFT_BITMAP_IDX)
    }

    #[inline]
    pub fn get_left(&self) -> &SingleBufferCF<'registered, D> {
        &self.left
    }

    #[inline]
    pub fn set_left(&mut self, field: SingleBufferCF<'registered, D>) {
        self.bitmap[Self::LEFT_BITMAP_OFFSET].set(Self::LEFT_BITMAP_IDX, true);
        self.left = field;
    }

    #[inline]
    pub fn get_mut_left(&mut self) -> &mut SingleBufferCF<'registered, D> {
        &mut self.left
    }

    #[inline]
    pub fn has_right(&self) -> bool {
        self.bitmap[Self::RIGHT_BITMAP_OFFSET].get(Self::RIGHT_BITMAP_IDX)
    }

    #[inline]
    pub fn get_right(&self) -> &SingleBufferCF<'registered, D> {
        &self.right
    }

    #[inline]
    pub fn set_right(&mut self, field: SingleBufferCF<'registered, D>) {
        self.bitmap[Self::RIGHT_BITMAP_OFFSET].set(Self::RIGHT_BITMAP_IDX, true);
        self.right = field;
    }

    #[inline]
    pub fn get_mut_right(&mut self) -> &mut SingleBufferCF<'registered, D> {
        &mut self.right
    }
}

impl<'arena, 'registered, D> HybridArenaRcSgaHdr<'arena, D> for Tree1LCF<'registered, D>
where
    D: Datapath,
{
    const NUMBER_OF_FIELDS: usize = 2;

    const CONSTANT_HEADER_SIZE: usize = SIZE_FIELD + OFFSET_FIELD;

    const NUM_U32_BITMAPS: usize = Tree1LCF_NUM_U32_BITMAPS;
    #[inline]
    fn new_in(arena: &'arena bumpalo::Bump) -> Self
    where
        Self: Sized,
    {
        Tree1LCF {
            bitmap: [Bitmap::<32>::new(); Tree1LCF_NUM_U32_BITMAPS],
            left: SingleBufferCF::new_in(arena),
            right: SingleBufferCF::new_in(arena),
        }
    }

    #[inline]
    fn dynamic_header_size(&self) -> usize {
        BITMAP_LENGTH_FIELD
            + Self::bitmap_length()
            + self.get_bitmap_field(Self::LEFT_BITMAP_IDX, Self::LEFT_BITMAP_OFFSET) as usize
                * self.left.total_header_size(true, true)
            + self.get_bitmap_field(Self::RIGHT_BITMAP_IDX, Self::RIGHT_BITMAP_OFFSET) as usize
                * self.right.total_header_size(true, true)
    }

    #[inline]
    fn dynamic_header_start(&self) -> usize {
        BITMAP_LENGTH_FIELD
            + Self::bitmap_length()
            + self.get_bitmap_field(Self::LEFT_BITMAP_IDX, Self::LEFT_BITMAP_OFFSET) as usize
                * SingleBufferCF::<D>::CONSTANT_HEADER_SIZE
            + self.get_bitmap_field(Self::RIGHT_BITMAP_IDX, Self::RIGHT_BITMAP_OFFSET) as usize
                * SingleBufferCF::<D>::CONSTANT_HEADER_SIZE
    }

    #[inline]
    fn num_zero_copy_scatter_gather_entries(&self) -> usize {
        0 + self.left.num_zero_copy_scatter_gather_entries()
            + self.right.num_zero_copy_scatter_gather_entries()
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

    #[inline]
    fn check_deep_equality(&self, other: &Self) -> bool {
        if self.get_bitmap_field(Self::LEFT_BITMAP_IDX, Self::LEFT_BITMAP_OFFSET)
            != other.get_bitmap_field(Self::LEFT_BITMAP_IDX, Self::LEFT_BITMAP_OFFSET)
        {
            return false;
        } else if self.get_bitmap_field(Self::LEFT_BITMAP_IDX, Self::LEFT_BITMAP_OFFSET)
            && other.get_bitmap_field(Self::LEFT_BITMAP_IDX, Self::LEFT_BITMAP_OFFSET)
        {
            if !self.get_left().check_deep_equality(&other.get_left()) {
                return false;
            }
        }

        if self.get_bitmap_field(Self::RIGHT_BITMAP_IDX, Self::RIGHT_BITMAP_OFFSET)
            != other.get_bitmap_field(Self::RIGHT_BITMAP_IDX, Self::RIGHT_BITMAP_OFFSET)
        {
            return false;
        } else if self.get_bitmap_field(Self::RIGHT_BITMAP_IDX, Self::RIGHT_BITMAP_OFFSET)
            && other.get_bitmap_field(Self::RIGHT_BITMAP_IDX, Self::RIGHT_BITMAP_OFFSET)
        {
            if !self.get_right().check_deep_equality(&other.get_right()) {
                return false;
            }
        }

        return true;
    }

    #[inline]
    fn iterate_over_entries<F>(
        &self,
        copy_context: &mut CopyContext<'arena, D>,
        header_len: usize,
        header_buffer: &mut [u8],
        constant_header_offset: usize,
        dynamic_header_offset: usize,
        cur_entry_ptr: &mut usize,
        datapath_callback: &mut F,
        callback_state: &mut D::CallbackEntryState,
    ) -> Result<usize>
    where
        F: FnMut(&D::DatapathMetadata, &mut D::CallbackEntryState) -> Result<()>,
    {
        self.serialize_bitmap(header_buffer, constant_header_offset);
        let mut cur_constant_offset =
            constant_header_offset + BITMAP_LENGTH_FIELD + Self::bitmap_length();

        let mut cur_dynamic_offset = dynamic_header_offset;
        let mut ret = 0;

        if self.get_bitmap_field(Self::LEFT_BITMAP_IDX, Self::LEFT_BITMAP_OFFSET) {
            write_size_and_offset(cur_constant_offset, 0, cur_dynamic_offset, header_buffer);
            ret += self.left.iterate_over_entries(
                copy_context,
                header_len,
                header_buffer,
                constant_header_offset,
                dynamic_header_offset,
                cur_entry_ptr,
                datapath_callback,
                callback_state,
            )?;

            cur_constant_offset += SingleBufferCF::<D>::CONSTANT_HEADER_SIZE;
            cur_dynamic_offset += self.left.dynamic_header_size();
        }

        if self.get_bitmap_field(Self::RIGHT_BITMAP_IDX, Self::RIGHT_BITMAP_OFFSET) {
            write_size_and_offset(cur_constant_offset, 0, cur_dynamic_offset, header_buffer);
            ret += self.right.iterate_over_entries(
                copy_context,
                header_len,
                header_buffer,
                constant_header_offset,
                dynamic_header_offset,
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
        datapath: &mut D,
        header: &mut [u8],
        constant_header_offset: usize,
        dynamic_header_start: usize,
        copy_context: &mut CopyContext<'a, D>,
        zero_copy_scatter_gather_entries: &mut [D::DatapathMetadata],
        ds_offset: &mut usize,
    ) -> Result<()> {
        self.serialize_bitmap(header, constant_header_offset);
        let mut cur_constant_offset =
            constant_header_offset + BITMAP_LENGTH_FIELD + Self::bitmap_length();

        let mut cur_dynamic_offset = dynamic_header_start;
        let mut cur_sge_idx = 0;

        if self.get_bitmap_field(Self::LEFT_BITMAP_IDX, Self::LEFT_BITMAP_OFFSET) {
            write_size_and_offset(cur_constant_offset, 0, cur_dynamic_offset, header);
            self.left.inner_serialize(
                datapath,
                header,
                cur_dynamic_offset,
                cur_dynamic_offset + self.left.dynamic_header_start(),
                copy_context,
                &mut zero_copy_scatter_gather_entries
                    [cur_sge_idx..(cur_sge_idx + self.left.num_zero_copy_scatter_gather_entries())],
                ds_offset,
            )?;

            cur_sge_idx += self.left.num_zero_copy_scatter_gather_entries();
            cur_constant_offset += SingleBufferCF::<D>::CONSTANT_HEADER_SIZE;
            cur_dynamic_offset += self.left.dynamic_header_size();
        }

        if self.get_bitmap_field(Self::RIGHT_BITMAP_IDX, Self::RIGHT_BITMAP_OFFSET) {
            write_size_and_offset(cur_constant_offset, 0, cur_dynamic_offset, header);
            self.right.inner_serialize(
                datapath,
                header,
                cur_dynamic_offset,
                cur_dynamic_offset + self.right.dynamic_header_start(),
                copy_context,
                &mut zero_copy_scatter_gather_entries[cur_sge_idx
                    ..(cur_sge_idx + self.right.num_zero_copy_scatter_gather_entries())],
                ds_offset,
            )?;
        }

        Ok(())
    }

    #[inline]
    fn inner_deserialize(
        &mut self,
        buffer: &D::DatapathMetadata,
        header_offset: usize,
        buffer_offset: usize,
        arena: &'arena bumpalo::Bump,
    ) -> Result<()> {
        let bitmap_size = self.deserialize_bitmap(buffer, header_offset, buffer_offset);
        let mut cur_constant_offset = header_offset + BITMAP_LENGTH_FIELD + bitmap_size;

        if self.get_bitmap_field(Self::LEFT_BITMAP_IDX, Self::LEFT_BITMAP_OFFSET) {
            self.left.inner_deserialize(
                buffer,
                read_size_and_offset::<D>(cur_constant_offset, buffer)?.1,
                buffer_offset,
                arena,
            )?;
            cur_constant_offset += SingleBufferCF::<D>::CONSTANT_HEADER_SIZE;
        }

        if self.get_bitmap_field(Self::RIGHT_BITMAP_IDX, Self::RIGHT_BITMAP_OFFSET) {
            self.right.inner_deserialize(
                buffer,
                read_size_and_offset::<D>(cur_constant_offset, buffer)?.1,
                buffer_offset,
                arena,
            )?;
        }

        Ok(())
    }
}

pub const Tree2LCF_NUM_U32_BITMAPS: usize = 1;

pub struct Tree2LCF<'registered, D>
where
    D: Datapath,
{
    bitmap: [Bitmap<32>; Tree2LCF_NUM_U32_BITMAPS],
    left: Tree1LCF<'registered, D>,
    right: Tree1LCF<'registered, D>,
}

impl<'registered, D> Clone for Tree2LCF<'registered, D>
where
    D: Datapath,
{
    #[inline]
    fn clone(&self) -> Self {
        Tree2LCF {
            bitmap: self.bitmap.clone(),
            left: self.left.clone(),
            right: self.right.clone(),
        }
    }
}

impl<'registered, D> std::fmt::Debug for Tree2LCF<'registered, D>
where
    D: Datapath,
{
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tree2LCF")
            .field("left", &self.left)
            .field("right", &self.right)
            .finish()
    }
}

impl<'registered, D> Tree2LCF<'registered, D>
where
    D: Datapath,
{
    const LEFT_BITMAP_IDX: usize = 0;
    const LEFT_BITMAP_OFFSET: usize = 0;

    const RIGHT_BITMAP_IDX: usize = 1;
    const RIGHT_BITMAP_OFFSET: usize = 0;

    #[inline]
    pub fn has_left(&self) -> bool {
        self.bitmap[Self::LEFT_BITMAP_OFFSET].get(Self::LEFT_BITMAP_IDX)
    }

    #[inline]
    pub fn get_left(&self) -> &Tree1LCF<'registered, D> {
        &self.left
    }

    #[inline]
    pub fn set_left(&mut self, field: Tree1LCF<'registered, D>) {
        self.bitmap[Self::LEFT_BITMAP_OFFSET].set(Self::LEFT_BITMAP_IDX, true);
        self.left = field;
    }

    #[inline]
    pub fn get_mut_left(&mut self) -> &mut Tree1LCF<'registered, D> {
        &mut self.left
    }

    #[inline]
    pub fn has_right(&self) -> bool {
        self.bitmap[Self::RIGHT_BITMAP_OFFSET].get(Self::RIGHT_BITMAP_IDX)
    }

    #[inline]
    pub fn get_right(&self) -> &Tree1LCF<'registered, D> {
        &self.right
    }

    #[inline]
    pub fn set_right(&mut self, field: Tree1LCF<'registered, D>) {
        self.bitmap[Self::RIGHT_BITMAP_OFFSET].set(Self::RIGHT_BITMAP_IDX, true);
        self.right = field;
    }

    #[inline]
    pub fn get_mut_right(&mut self) -> &mut Tree1LCF<'registered, D> {
        &mut self.right
    }
}

impl<'arena, 'registered, D> HybridArenaRcSgaHdr<'arena, D> for Tree2LCF<'registered, D>
where
    D: Datapath,
{
    const NUMBER_OF_FIELDS: usize = 2;

    const CONSTANT_HEADER_SIZE: usize = SIZE_FIELD + OFFSET_FIELD;

    const NUM_U32_BITMAPS: usize = Tree2LCF_NUM_U32_BITMAPS;
    #[inline]
    fn new_in(arena: &'arena bumpalo::Bump) -> Self
    where
        Self: Sized,
    {
        Tree2LCF {
            bitmap: [Bitmap::<32>::new(); Tree2LCF_NUM_U32_BITMAPS],
            left: Tree1LCF::new_in(arena),
            right: Tree1LCF::new_in(arena),
        }
    }

    #[inline]
    fn dynamic_header_size(&self) -> usize {
        BITMAP_LENGTH_FIELD
            + Self::bitmap_length()
            + self.get_bitmap_field(Self::LEFT_BITMAP_IDX, Self::LEFT_BITMAP_OFFSET) as usize
                * self.left.total_header_size(true, true)
            + self.get_bitmap_field(Self::RIGHT_BITMAP_IDX, Self::RIGHT_BITMAP_OFFSET) as usize
                * self.right.total_header_size(true, true)
    }

    #[inline]
    fn dynamic_header_start(&self) -> usize {
        BITMAP_LENGTH_FIELD
            + Self::bitmap_length()
            + self.get_bitmap_field(Self::LEFT_BITMAP_IDX, Self::LEFT_BITMAP_OFFSET) as usize
                * Tree1LCF::<D>::CONSTANT_HEADER_SIZE
            + self.get_bitmap_field(Self::RIGHT_BITMAP_IDX, Self::RIGHT_BITMAP_OFFSET) as usize
                * Tree1LCF::<D>::CONSTANT_HEADER_SIZE
    }

    #[inline]
    fn num_zero_copy_scatter_gather_entries(&self) -> usize {
        0 + self.left.num_zero_copy_scatter_gather_entries()
            + self.right.num_zero_copy_scatter_gather_entries()
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

    #[inline]
    fn check_deep_equality(&self, other: &Self) -> bool {
        if self.get_bitmap_field(Self::LEFT_BITMAP_IDX, Self::LEFT_BITMAP_OFFSET)
            != other.get_bitmap_field(Self::LEFT_BITMAP_IDX, Self::LEFT_BITMAP_OFFSET)
        {
            return false;
        } else if self.get_bitmap_field(Self::LEFT_BITMAP_IDX, Self::LEFT_BITMAP_OFFSET)
            && other.get_bitmap_field(Self::LEFT_BITMAP_IDX, Self::LEFT_BITMAP_OFFSET)
        {
            if !self.get_left().check_deep_equality(&other.get_left()) {
                return false;
            }
        }

        if self.get_bitmap_field(Self::RIGHT_BITMAP_IDX, Self::RIGHT_BITMAP_OFFSET)
            != other.get_bitmap_field(Self::RIGHT_BITMAP_IDX, Self::RIGHT_BITMAP_OFFSET)
        {
            return false;
        } else if self.get_bitmap_field(Self::RIGHT_BITMAP_IDX, Self::RIGHT_BITMAP_OFFSET)
            && other.get_bitmap_field(Self::RIGHT_BITMAP_IDX, Self::RIGHT_BITMAP_OFFSET)
        {
            if !self.get_right().check_deep_equality(&other.get_right()) {
                return false;
            }
        }

        return true;
    }

    #[inline]
    fn iterate_over_entries<F>(
        &self,
        copy_context: &mut CopyContext<'arena, D>,
        header_len: usize,
        header_buffer: &mut [u8],
        constant_header_offset: usize,
        dynamic_header_offset: usize,
        cur_entry_ptr: &mut usize,
        datapath_callback: &mut F,
        callback_state: &mut D::CallbackEntryState,
    ) -> Result<usize>
    where
        F: FnMut(&D::DatapathMetadata, &mut D::CallbackEntryState) -> Result<()>,
    {
        self.serialize_bitmap(header_buffer, constant_header_offset);
        let mut cur_constant_offset =
            constant_header_offset + BITMAP_LENGTH_FIELD + Self::bitmap_length();

        let mut cur_dynamic_offset = dynamic_header_offset;
        let mut ret = 0;

        if self.get_bitmap_field(Self::LEFT_BITMAP_IDX, Self::LEFT_BITMAP_OFFSET) {
            write_size_and_offset(cur_constant_offset, 0, cur_dynamic_offset, header_buffer);
            ret += self.left.iterate_over_entries(
                copy_context,
                header_len,
                header_buffer,
                constant_header_offset,
                dynamic_header_offset,
                cur_entry_ptr,
                datapath_callback,
                callback_state,
            )?;

            cur_constant_offset += Tree1LCF::<D>::CONSTANT_HEADER_SIZE;
            cur_dynamic_offset += self.left.dynamic_header_size();
        }

        if self.get_bitmap_field(Self::RIGHT_BITMAP_IDX, Self::RIGHT_BITMAP_OFFSET) {
            write_size_and_offset(cur_constant_offset, 0, cur_dynamic_offset, header_buffer);
            ret += self.right.iterate_over_entries(
                copy_context,
                header_len,
                header_buffer,
                constant_header_offset,
                dynamic_header_offset,
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
        datapath: &mut D,
        header: &mut [u8],
        constant_header_offset: usize,
        dynamic_header_start: usize,
        copy_context: &mut CopyContext<'a, D>,
        zero_copy_scatter_gather_entries: &mut [D::DatapathMetadata],
        ds_offset: &mut usize,
    ) -> Result<()> {
        self.serialize_bitmap(header, constant_header_offset);
        let mut cur_constant_offset =
            constant_header_offset + BITMAP_LENGTH_FIELD + Self::bitmap_length();

        let mut cur_dynamic_offset = dynamic_header_start;
        let mut cur_sge_idx = 0;

        if self.get_bitmap_field(Self::LEFT_BITMAP_IDX, Self::LEFT_BITMAP_OFFSET) {
            write_size_and_offset(cur_constant_offset, 0, cur_dynamic_offset, header);
            self.left.inner_serialize(
                datapath,
                header,
                cur_dynamic_offset,
                cur_dynamic_offset + self.left.dynamic_header_start(),
                copy_context,
                &mut zero_copy_scatter_gather_entries
                    [cur_sge_idx..(cur_sge_idx + self.left.num_zero_copy_scatter_gather_entries())],
                ds_offset,
            )?;

            cur_sge_idx += self.left.num_zero_copy_scatter_gather_entries();
            cur_constant_offset += Tree1LCF::<D>::CONSTANT_HEADER_SIZE;
            cur_dynamic_offset += self.left.dynamic_header_size();
        }

        if self.get_bitmap_field(Self::RIGHT_BITMAP_IDX, Self::RIGHT_BITMAP_OFFSET) {
            write_size_and_offset(cur_constant_offset, 0, cur_dynamic_offset, header);
            self.right.inner_serialize(
                datapath,
                header,
                cur_dynamic_offset,
                cur_dynamic_offset + self.right.dynamic_header_start(),
                copy_context,
                &mut zero_copy_scatter_gather_entries[cur_sge_idx
                    ..(cur_sge_idx + self.right.num_zero_copy_scatter_gather_entries())],
                ds_offset,
            )?;
        }

        Ok(())
    }

    #[inline]
    fn inner_deserialize(
        &mut self,
        buffer: &D::DatapathMetadata,
        header_offset: usize,
        buffer_offset: usize,
        arena: &'arena bumpalo::Bump,
    ) -> Result<()> {
        let bitmap_size = self.deserialize_bitmap(buffer, header_offset, buffer_offset);
        let mut cur_constant_offset = header_offset + BITMAP_LENGTH_FIELD + bitmap_size;

        if self.get_bitmap_field(Self::LEFT_BITMAP_IDX, Self::LEFT_BITMAP_OFFSET) {
            self.left.inner_deserialize(
                buffer,
                read_size_and_offset::<D>(cur_constant_offset, buffer)?.1,
                buffer_offset,
                arena,
            )?;
            cur_constant_offset += Tree1LCF::<D>::CONSTANT_HEADER_SIZE;
        }

        if self.get_bitmap_field(Self::RIGHT_BITMAP_IDX, Self::RIGHT_BITMAP_OFFSET) {
            self.right.inner_deserialize(
                buffer,
                read_size_and_offset::<D>(cur_constant_offset, buffer)?.1,
                buffer_offset,
                arena,
            )?;
        }

        Ok(())
    }
}

pub const Tree3LCF_NUM_U32_BITMAPS: usize = 1;

pub struct Tree3LCF<'registered, D>
where
    D: Datapath,
{
    bitmap: [Bitmap<32>; Tree3LCF_NUM_U32_BITMAPS],
    left: Tree2LCF<'registered, D>,
    right: Tree2LCF<'registered, D>,
}

impl<'registered, D> Clone for Tree3LCF<'registered, D>
where
    D: Datapath,
{
    #[inline]
    fn clone(&self) -> Self {
        Tree3LCF {
            bitmap: self.bitmap.clone(),
            left: self.left.clone(),
            right: self.right.clone(),
        }
    }
}

impl<'registered, D> std::fmt::Debug for Tree3LCF<'registered, D>
where
    D: Datapath,
{
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tree3LCF")
            .field("left", &self.left)
            .field("right", &self.right)
            .finish()
    }
}

impl<'registered, D> Tree3LCF<'registered, D>
where
    D: Datapath,
{
    const LEFT_BITMAP_IDX: usize = 0;
    const LEFT_BITMAP_OFFSET: usize = 0;

    const RIGHT_BITMAP_IDX: usize = 1;
    const RIGHT_BITMAP_OFFSET: usize = 0;

    #[inline]
    pub fn has_left(&self) -> bool {
        self.bitmap[Self::LEFT_BITMAP_OFFSET].get(Self::LEFT_BITMAP_IDX)
    }

    #[inline]
    pub fn get_left(&self) -> &Tree2LCF<'registered, D> {
        &self.left
    }

    #[inline]
    pub fn set_left(&mut self, field: Tree2LCF<'registered, D>) {
        self.bitmap[Self::LEFT_BITMAP_OFFSET].set(Self::LEFT_BITMAP_IDX, true);
        self.left = field;
    }

    #[inline]
    pub fn get_mut_left(&mut self) -> &mut Tree2LCF<'registered, D> {
        &mut self.left
    }

    #[inline]
    pub fn has_right(&self) -> bool {
        self.bitmap[Self::RIGHT_BITMAP_OFFSET].get(Self::RIGHT_BITMAP_IDX)
    }

    #[inline]
    pub fn get_right(&self) -> &Tree2LCF<'registered, D> {
        &self.right
    }

    #[inline]
    pub fn set_right(&mut self, field: Tree2LCF<'registered, D>) {
        self.bitmap[Self::RIGHT_BITMAP_OFFSET].set(Self::RIGHT_BITMAP_IDX, true);
        self.right = field;
    }

    #[inline]
    pub fn get_mut_right(&mut self) -> &mut Tree2LCF<'registered, D> {
        &mut self.right
    }
}

impl<'arena, 'registered, D> HybridArenaRcSgaHdr<'arena, D> for Tree3LCF<'registered, D>
where
    D: Datapath,
{
    const NUMBER_OF_FIELDS: usize = 2;

    const CONSTANT_HEADER_SIZE: usize = SIZE_FIELD + OFFSET_FIELD;

    const NUM_U32_BITMAPS: usize = Tree3LCF_NUM_U32_BITMAPS;
    #[inline]
    fn new_in(arena: &'arena bumpalo::Bump) -> Self
    where
        Self: Sized,
    {
        Tree3LCF {
            bitmap: [Bitmap::<32>::new(); Tree3LCF_NUM_U32_BITMAPS],
            left: Tree2LCF::new_in(arena),
            right: Tree2LCF::new_in(arena),
        }
    }

    #[inline]
    fn dynamic_header_size(&self) -> usize {
        BITMAP_LENGTH_FIELD
            + Self::bitmap_length()
            + self.get_bitmap_field(Self::LEFT_BITMAP_IDX, Self::LEFT_BITMAP_OFFSET) as usize
                * self.left.total_header_size(true, true)
            + self.get_bitmap_field(Self::RIGHT_BITMAP_IDX, Self::RIGHT_BITMAP_OFFSET) as usize
                * self.right.total_header_size(true, true)
    }

    #[inline]
    fn dynamic_header_start(&self) -> usize {
        BITMAP_LENGTH_FIELD
            + Self::bitmap_length()
            + self.get_bitmap_field(Self::LEFT_BITMAP_IDX, Self::LEFT_BITMAP_OFFSET) as usize
                * Tree2LCF::<D>::CONSTANT_HEADER_SIZE
            + self.get_bitmap_field(Self::RIGHT_BITMAP_IDX, Self::RIGHT_BITMAP_OFFSET) as usize
                * Tree2LCF::<D>::CONSTANT_HEADER_SIZE
    }

    #[inline]
    fn num_zero_copy_scatter_gather_entries(&self) -> usize {
        0 + self.left.num_zero_copy_scatter_gather_entries()
            + self.right.num_zero_copy_scatter_gather_entries()
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

    #[inline]
    fn check_deep_equality(&self, other: &Self) -> bool {
        if self.get_bitmap_field(Self::LEFT_BITMAP_IDX, Self::LEFT_BITMAP_OFFSET)
            != other.get_bitmap_field(Self::LEFT_BITMAP_IDX, Self::LEFT_BITMAP_OFFSET)
        {
            return false;
        } else if self.get_bitmap_field(Self::LEFT_BITMAP_IDX, Self::LEFT_BITMAP_OFFSET)
            && other.get_bitmap_field(Self::LEFT_BITMAP_IDX, Self::LEFT_BITMAP_OFFSET)
        {
            if !self.get_left().check_deep_equality(&other.get_left()) {
                return false;
            }
        }

        if self.get_bitmap_field(Self::RIGHT_BITMAP_IDX, Self::RIGHT_BITMAP_OFFSET)
            != other.get_bitmap_field(Self::RIGHT_BITMAP_IDX, Self::RIGHT_BITMAP_OFFSET)
        {
            return false;
        } else if self.get_bitmap_field(Self::RIGHT_BITMAP_IDX, Self::RIGHT_BITMAP_OFFSET)
            && other.get_bitmap_field(Self::RIGHT_BITMAP_IDX, Self::RIGHT_BITMAP_OFFSET)
        {
            if !self.get_right().check_deep_equality(&other.get_right()) {
                return false;
            }
        }

        return true;
    }

    #[inline]
    fn iterate_over_entries<F>(
        &self,
        copy_context: &mut CopyContext<'arena, D>,
        header_len: usize,
        header_buffer: &mut [u8],
        constant_header_offset: usize,
        dynamic_header_offset: usize,
        cur_entry_ptr: &mut usize,
        datapath_callback: &mut F,
        callback_state: &mut D::CallbackEntryState,
    ) -> Result<usize>
    where
        F: FnMut(&D::DatapathMetadata, &mut D::CallbackEntryState) -> Result<()>,
    {
        self.serialize_bitmap(header_buffer, constant_header_offset);
        let mut cur_constant_offset =
            constant_header_offset + BITMAP_LENGTH_FIELD + Self::bitmap_length();

        let mut cur_dynamic_offset = dynamic_header_offset;
        let mut ret = 0;

        if self.get_bitmap_field(Self::LEFT_BITMAP_IDX, Self::LEFT_BITMAP_OFFSET) {
            write_size_and_offset(cur_constant_offset, 0, cur_dynamic_offset, header_buffer);
            ret += self.left.iterate_over_entries(
                copy_context,
                header_len,
                header_buffer,
                constant_header_offset,
                dynamic_header_offset,
                cur_entry_ptr,
                datapath_callback,
                callback_state,
            )?;

            cur_constant_offset += Tree2LCF::<D>::CONSTANT_HEADER_SIZE;
            cur_dynamic_offset += self.left.dynamic_header_size();
        }

        if self.get_bitmap_field(Self::RIGHT_BITMAP_IDX, Self::RIGHT_BITMAP_OFFSET) {
            write_size_and_offset(cur_constant_offset, 0, cur_dynamic_offset, header_buffer);
            ret += self.right.iterate_over_entries(
                copy_context,
                header_len,
                header_buffer,
                constant_header_offset,
                dynamic_header_offset,
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
        datapath: &mut D,
        header: &mut [u8],
        constant_header_offset: usize,
        dynamic_header_start: usize,
        copy_context: &mut CopyContext<'a, D>,
        zero_copy_scatter_gather_entries: &mut [D::DatapathMetadata],
        ds_offset: &mut usize,
    ) -> Result<()> {
        self.serialize_bitmap(header, constant_header_offset);
        let mut cur_constant_offset =
            constant_header_offset + BITMAP_LENGTH_FIELD + Self::bitmap_length();

        let mut cur_dynamic_offset = dynamic_header_start;
        let mut cur_sge_idx = 0;

        if self.get_bitmap_field(Self::LEFT_BITMAP_IDX, Self::LEFT_BITMAP_OFFSET) {
            write_size_and_offset(cur_constant_offset, 0, cur_dynamic_offset, header);
            self.left.inner_serialize(
                datapath,
                header,
                cur_dynamic_offset,
                cur_dynamic_offset + self.left.dynamic_header_start(),
                copy_context,
                &mut zero_copy_scatter_gather_entries
                    [cur_sge_idx..(cur_sge_idx + self.left.num_zero_copy_scatter_gather_entries())],
                ds_offset,
            )?;

            cur_sge_idx += self.left.num_zero_copy_scatter_gather_entries();
            cur_constant_offset += Tree2LCF::<D>::CONSTANT_HEADER_SIZE;
            cur_dynamic_offset += self.left.dynamic_header_size();
        }

        if self.get_bitmap_field(Self::RIGHT_BITMAP_IDX, Self::RIGHT_BITMAP_OFFSET) {
            write_size_and_offset(cur_constant_offset, 0, cur_dynamic_offset, header);
            self.right.inner_serialize(
                datapath,
                header,
                cur_dynamic_offset,
                cur_dynamic_offset + self.right.dynamic_header_start(),
                copy_context,
                &mut zero_copy_scatter_gather_entries[cur_sge_idx
                    ..(cur_sge_idx + self.right.num_zero_copy_scatter_gather_entries())],
                ds_offset,
            )?;
        }

        Ok(())
    }

    #[inline]
    fn inner_deserialize(
        &mut self,
        buffer: &D::DatapathMetadata,
        header_offset: usize,
        buffer_offset: usize,
        arena: &'arena bumpalo::Bump,
    ) -> Result<()> {
        let bitmap_size = self.deserialize_bitmap(buffer, header_offset, buffer_offset);
        let mut cur_constant_offset = header_offset + BITMAP_LENGTH_FIELD + bitmap_size;

        if self.get_bitmap_field(Self::LEFT_BITMAP_IDX, Self::LEFT_BITMAP_OFFSET) {
            self.left.inner_deserialize(
                buffer,
                read_size_and_offset::<D>(cur_constant_offset, buffer)?.1,
                buffer_offset,
                arena,
            )?;
            cur_constant_offset += Tree2LCF::<D>::CONSTANT_HEADER_SIZE;
        }

        if self.get_bitmap_field(Self::RIGHT_BITMAP_IDX, Self::RIGHT_BITMAP_OFFSET) {
            self.right.inner_deserialize(
                buffer,
                read_size_and_offset::<D>(cur_constant_offset, buffer)?.1,
                buffer_offset,
                arena,
            )?;
        }

        Ok(())
    }
}

pub const Tree4LCF_NUM_U32_BITMAPS: usize = 1;

pub struct Tree4LCF<'registered, D>
where
    D: Datapath,
{
    bitmap: [Bitmap<32>; Tree4LCF_NUM_U32_BITMAPS],
    left: Tree3LCF<'registered, D>,
    right: Tree3LCF<'registered, D>,
}

impl<'registered, D> Clone for Tree4LCF<'registered, D>
where
    D: Datapath,
{
    #[inline]
    fn clone(&self) -> Self {
        Tree4LCF {
            bitmap: self.bitmap.clone(),
            left: self.left.clone(),
            right: self.right.clone(),
        }
    }
}

impl<'registered, D> std::fmt::Debug for Tree4LCF<'registered, D>
where
    D: Datapath,
{
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tree4LCF")
            .field("left", &self.left)
            .field("right", &self.right)
            .finish()
    }
}

impl<'registered, D> Tree4LCF<'registered, D>
where
    D: Datapath,
{
    const LEFT_BITMAP_IDX: usize = 0;
    const LEFT_BITMAP_OFFSET: usize = 0;

    const RIGHT_BITMAP_IDX: usize = 1;
    const RIGHT_BITMAP_OFFSET: usize = 0;

    #[inline]
    pub fn has_left(&self) -> bool {
        self.bitmap[Self::LEFT_BITMAP_OFFSET].get(Self::LEFT_BITMAP_IDX)
    }

    #[inline]
    pub fn get_left(&self) -> &Tree3LCF<'registered, D> {
        &self.left
    }

    #[inline]
    pub fn set_left(&mut self, field: Tree3LCF<'registered, D>) {
        self.bitmap[Self::LEFT_BITMAP_OFFSET].set(Self::LEFT_BITMAP_IDX, true);
        self.left = field;
    }

    #[inline]
    pub fn get_mut_left(&mut self) -> &mut Tree3LCF<'registered, D> {
        &mut self.left
    }

    #[inline]
    pub fn has_right(&self) -> bool {
        self.bitmap[Self::RIGHT_BITMAP_OFFSET].get(Self::RIGHT_BITMAP_IDX)
    }

    #[inline]
    pub fn get_right(&self) -> &Tree3LCF<'registered, D> {
        &self.right
    }

    #[inline]
    pub fn set_right(&mut self, field: Tree3LCF<'registered, D>) {
        self.bitmap[Self::RIGHT_BITMAP_OFFSET].set(Self::RIGHT_BITMAP_IDX, true);
        self.right = field;
    }

    #[inline]
    pub fn get_mut_right(&mut self) -> &mut Tree3LCF<'registered, D> {
        &mut self.right
    }
}

impl<'arena, 'registered, D> HybridArenaRcSgaHdr<'arena, D> for Tree4LCF<'registered, D>
where
    D: Datapath,
{
    const NUMBER_OF_FIELDS: usize = 2;

    const CONSTANT_HEADER_SIZE: usize = SIZE_FIELD + OFFSET_FIELD;

    const NUM_U32_BITMAPS: usize = Tree4LCF_NUM_U32_BITMAPS;
    #[inline]
    fn new_in(arena: &'arena bumpalo::Bump) -> Self
    where
        Self: Sized,
    {
        Tree4LCF {
            bitmap: [Bitmap::<32>::new(); Tree4LCF_NUM_U32_BITMAPS],
            left: Tree3LCF::new_in(arena),
            right: Tree3LCF::new_in(arena),
        }
    }

    #[inline]
    fn dynamic_header_size(&self) -> usize {
        BITMAP_LENGTH_FIELD
            + Self::bitmap_length()
            + self.get_bitmap_field(Self::LEFT_BITMAP_IDX, Self::LEFT_BITMAP_OFFSET) as usize
                * self.left.total_header_size(true, true)
            + self.get_bitmap_field(Self::RIGHT_BITMAP_IDX, Self::RIGHT_BITMAP_OFFSET) as usize
                * self.right.total_header_size(true, true)
    }

    #[inline]
    fn dynamic_header_start(&self) -> usize {
        BITMAP_LENGTH_FIELD
            + Self::bitmap_length()
            + self.get_bitmap_field(Self::LEFT_BITMAP_IDX, Self::LEFT_BITMAP_OFFSET) as usize
                * Tree3LCF::<D>::CONSTANT_HEADER_SIZE
            + self.get_bitmap_field(Self::RIGHT_BITMAP_IDX, Self::RIGHT_BITMAP_OFFSET) as usize
                * Tree3LCF::<D>::CONSTANT_HEADER_SIZE
    }

    #[inline]
    fn num_zero_copy_scatter_gather_entries(&self) -> usize {
        0 + self.left.num_zero_copy_scatter_gather_entries()
            + self.right.num_zero_copy_scatter_gather_entries()
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

    #[inline]
    fn check_deep_equality(&self, other: &Self) -> bool {
        if self.get_bitmap_field(Self::LEFT_BITMAP_IDX, Self::LEFT_BITMAP_OFFSET)
            != other.get_bitmap_field(Self::LEFT_BITMAP_IDX, Self::LEFT_BITMAP_OFFSET)
        {
            return false;
        } else if self.get_bitmap_field(Self::LEFT_BITMAP_IDX, Self::LEFT_BITMAP_OFFSET)
            && other.get_bitmap_field(Self::LEFT_BITMAP_IDX, Self::LEFT_BITMAP_OFFSET)
        {
            if !self.get_left().check_deep_equality(&other.get_left()) {
                return false;
            }
        }

        if self.get_bitmap_field(Self::RIGHT_BITMAP_IDX, Self::RIGHT_BITMAP_OFFSET)
            != other.get_bitmap_field(Self::RIGHT_BITMAP_IDX, Self::RIGHT_BITMAP_OFFSET)
        {
            return false;
        } else if self.get_bitmap_field(Self::RIGHT_BITMAP_IDX, Self::RIGHT_BITMAP_OFFSET)
            && other.get_bitmap_field(Self::RIGHT_BITMAP_IDX, Self::RIGHT_BITMAP_OFFSET)
        {
            if !self.get_right().check_deep_equality(&other.get_right()) {
                return false;
            }
        }

        return true;
    }

    #[inline]
    fn iterate_over_entries<F>(
        &self,
        copy_context: &mut CopyContext<'arena, D>,
        header_len: usize,
        header_buffer: &mut [u8],
        constant_header_offset: usize,
        dynamic_header_offset: usize,
        cur_entry_ptr: &mut usize,
        datapath_callback: &mut F,
        callback_state: &mut D::CallbackEntryState,
    ) -> Result<usize>
    where
        F: FnMut(&D::DatapathMetadata, &mut D::CallbackEntryState) -> Result<()>,
    {
        self.serialize_bitmap(header_buffer, constant_header_offset);
        let mut cur_constant_offset =
            constant_header_offset + BITMAP_LENGTH_FIELD + Self::bitmap_length();

        let mut cur_dynamic_offset = dynamic_header_offset;
        let mut ret = 0;

        if self.get_bitmap_field(Self::LEFT_BITMAP_IDX, Self::LEFT_BITMAP_OFFSET) {
            write_size_and_offset(cur_constant_offset, 0, cur_dynamic_offset, header_buffer);
            ret += self.left.iterate_over_entries(
                copy_context,
                header_len,
                header_buffer,
                constant_header_offset,
                dynamic_header_offset,
                cur_entry_ptr,
                datapath_callback,
                callback_state,
            )?;

            cur_constant_offset += Tree3LCF::<D>::CONSTANT_HEADER_SIZE;
            cur_dynamic_offset += self.left.dynamic_header_size();
        }

        if self.get_bitmap_field(Self::RIGHT_BITMAP_IDX, Self::RIGHT_BITMAP_OFFSET) {
            write_size_and_offset(cur_constant_offset, 0, cur_dynamic_offset, header_buffer);
            ret += self.right.iterate_over_entries(
                copy_context,
                header_len,
                header_buffer,
                constant_header_offset,
                dynamic_header_offset,
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
        datapath: &mut D,
        header: &mut [u8],
        constant_header_offset: usize,
        dynamic_header_start: usize,
        copy_context: &mut CopyContext<'a, D>,
        zero_copy_scatter_gather_entries: &mut [D::DatapathMetadata],
        ds_offset: &mut usize,
    ) -> Result<()> {
        self.serialize_bitmap(header, constant_header_offset);
        let mut cur_constant_offset =
            constant_header_offset + BITMAP_LENGTH_FIELD + Self::bitmap_length();

        let mut cur_dynamic_offset = dynamic_header_start;
        let mut cur_sge_idx = 0;

        if self.get_bitmap_field(Self::LEFT_BITMAP_IDX, Self::LEFT_BITMAP_OFFSET) {
            write_size_and_offset(cur_constant_offset, 0, cur_dynamic_offset, header);
            self.left.inner_serialize(
                datapath,
                header,
                cur_dynamic_offset,
                cur_dynamic_offset + self.left.dynamic_header_start(),
                copy_context,
                &mut zero_copy_scatter_gather_entries
                    [cur_sge_idx..(cur_sge_idx + self.left.num_zero_copy_scatter_gather_entries())],
                ds_offset,
            )?;

            cur_sge_idx += self.left.num_zero_copy_scatter_gather_entries();
            cur_constant_offset += Tree3LCF::<D>::CONSTANT_HEADER_SIZE;
            cur_dynamic_offset += self.left.dynamic_header_size();
        }

        if self.get_bitmap_field(Self::RIGHT_BITMAP_IDX, Self::RIGHT_BITMAP_OFFSET) {
            write_size_and_offset(cur_constant_offset, 0, cur_dynamic_offset, header);
            self.right.inner_serialize(
                datapath,
                header,
                cur_dynamic_offset,
                cur_dynamic_offset + self.right.dynamic_header_start(),
                copy_context,
                &mut zero_copy_scatter_gather_entries[cur_sge_idx
                    ..(cur_sge_idx + self.right.num_zero_copy_scatter_gather_entries())],
                ds_offset,
            )?;
        }

        Ok(())
    }

    #[inline]
    fn inner_deserialize(
        &mut self,
        buffer: &D::DatapathMetadata,
        header_offset: usize,
        buffer_offset: usize,
        arena: &'arena bumpalo::Bump,
    ) -> Result<()> {
        let bitmap_size = self.deserialize_bitmap(buffer, header_offset, buffer_offset);
        let mut cur_constant_offset = header_offset + BITMAP_LENGTH_FIELD + bitmap_size;

        if self.get_bitmap_field(Self::LEFT_BITMAP_IDX, Self::LEFT_BITMAP_OFFSET) {
            self.left.inner_deserialize(
                buffer,
                read_size_and_offset::<D>(cur_constant_offset, buffer)?.1,
                buffer_offset,
                arena,
            )?;
            cur_constant_offset += Tree3LCF::<D>::CONSTANT_HEADER_SIZE;
        }

        if self.get_bitmap_field(Self::RIGHT_BITMAP_IDX, Self::RIGHT_BITMAP_OFFSET) {
            self.right.inner_deserialize(
                buffer,
                read_size_and_offset::<D>(cur_constant_offset, buffer)?.1,
                buffer_offset,
                arena,
            )?;
        }

        Ok(())
    }
}

pub const Tree5LCF_NUM_U32_BITMAPS: usize = 1;

pub struct Tree5LCF<'registered, D>
where
    D: Datapath,
{
    bitmap: [Bitmap<32>; Tree5LCF_NUM_U32_BITMAPS],
    left: Tree4LCF<'registered, D>,
    right: Tree4LCF<'registered, D>,
}

impl<'registered, D> Clone for Tree5LCF<'registered, D>
where
    D: Datapath,
{
    #[inline]
    fn clone(&self) -> Self {
        Tree5LCF {
            bitmap: self.bitmap.clone(),
            left: self.left.clone(),
            right: self.right.clone(),
        }
    }
}

impl<'registered, D> std::fmt::Debug for Tree5LCF<'registered, D>
where
    D: Datapath,
{
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tree5LCF")
            .field("left", &self.left)
            .field("right", &self.right)
            .finish()
    }
}

impl<'registered, D> Tree5LCF<'registered, D>
where
    D: Datapath,
{
    const LEFT_BITMAP_IDX: usize = 0;
    const LEFT_BITMAP_OFFSET: usize = 0;

    const RIGHT_BITMAP_IDX: usize = 1;
    const RIGHT_BITMAP_OFFSET: usize = 0;

    #[inline]
    pub fn has_left(&self) -> bool {
        self.bitmap[Self::LEFT_BITMAP_OFFSET].get(Self::LEFT_BITMAP_IDX)
    }

    #[inline]
    pub fn get_left(&self) -> &Tree4LCF<'registered, D> {
        &self.left
    }

    #[inline]
    pub fn set_left(&mut self, field: Tree4LCF<'registered, D>) {
        self.bitmap[Self::LEFT_BITMAP_OFFSET].set(Self::LEFT_BITMAP_IDX, true);
        self.left = field;
    }

    #[inline]
    pub fn get_mut_left(&mut self) -> &mut Tree4LCF<'registered, D> {
        &mut self.left
    }

    #[inline]
    pub fn has_right(&self) -> bool {
        self.bitmap[Self::RIGHT_BITMAP_OFFSET].get(Self::RIGHT_BITMAP_IDX)
    }

    #[inline]
    pub fn get_right(&self) -> &Tree4LCF<'registered, D> {
        &self.right
    }

    #[inline]
    pub fn set_right(&mut self, field: Tree4LCF<'registered, D>) {
        self.bitmap[Self::RIGHT_BITMAP_OFFSET].set(Self::RIGHT_BITMAP_IDX, true);
        self.right = field;
    }

    #[inline]
    pub fn get_mut_right(&mut self) -> &mut Tree4LCF<'registered, D> {
        &mut self.right
    }
}

impl<'arena, 'registered, D> HybridArenaRcSgaHdr<'arena, D> for Tree5LCF<'registered, D>
where
    D: Datapath,
{
    const NUMBER_OF_FIELDS: usize = 2;

    const CONSTANT_HEADER_SIZE: usize = SIZE_FIELD + OFFSET_FIELD;

    const NUM_U32_BITMAPS: usize = Tree5LCF_NUM_U32_BITMAPS;
    #[inline]
    fn new_in(arena: &'arena bumpalo::Bump) -> Self
    where
        Self: Sized,
    {
        Tree5LCF {
            bitmap: [Bitmap::<32>::new(); Tree5LCF_NUM_U32_BITMAPS],
            left: Tree4LCF::new_in(arena),
            right: Tree4LCF::new_in(arena),
        }
    }

    #[inline]
    fn dynamic_header_size(&self) -> usize {
        BITMAP_LENGTH_FIELD
            + Self::bitmap_length()
            + self.get_bitmap_field(Self::LEFT_BITMAP_IDX, Self::LEFT_BITMAP_OFFSET) as usize
                * self.left.total_header_size(true, true)
            + self.get_bitmap_field(Self::RIGHT_BITMAP_IDX, Self::RIGHT_BITMAP_OFFSET) as usize
                * self.right.total_header_size(true, true)
    }

    #[inline]
    fn dynamic_header_start(&self) -> usize {
        BITMAP_LENGTH_FIELD
            + Self::bitmap_length()
            + self.get_bitmap_field(Self::LEFT_BITMAP_IDX, Self::LEFT_BITMAP_OFFSET) as usize
                * Tree4LCF::<D>::CONSTANT_HEADER_SIZE
            + self.get_bitmap_field(Self::RIGHT_BITMAP_IDX, Self::RIGHT_BITMAP_OFFSET) as usize
                * Tree4LCF::<D>::CONSTANT_HEADER_SIZE
    }

    #[inline]
    fn num_zero_copy_scatter_gather_entries(&self) -> usize {
        0 + self.left.num_zero_copy_scatter_gather_entries()
            + self.right.num_zero_copy_scatter_gather_entries()
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

    #[inline]
    fn check_deep_equality(&self, other: &Self) -> bool {
        if self.get_bitmap_field(Self::LEFT_BITMAP_IDX, Self::LEFT_BITMAP_OFFSET)
            != other.get_bitmap_field(Self::LEFT_BITMAP_IDX, Self::LEFT_BITMAP_OFFSET)
        {
            return false;
        } else if self.get_bitmap_field(Self::LEFT_BITMAP_IDX, Self::LEFT_BITMAP_OFFSET)
            && other.get_bitmap_field(Self::LEFT_BITMAP_IDX, Self::LEFT_BITMAP_OFFSET)
        {
            if !self.get_left().check_deep_equality(&other.get_left()) {
                return false;
            }
        }

        if self.get_bitmap_field(Self::RIGHT_BITMAP_IDX, Self::RIGHT_BITMAP_OFFSET)
            != other.get_bitmap_field(Self::RIGHT_BITMAP_IDX, Self::RIGHT_BITMAP_OFFSET)
        {
            return false;
        } else if self.get_bitmap_field(Self::RIGHT_BITMAP_IDX, Self::RIGHT_BITMAP_OFFSET)
            && other.get_bitmap_field(Self::RIGHT_BITMAP_IDX, Self::RIGHT_BITMAP_OFFSET)
        {
            if !self.get_right().check_deep_equality(&other.get_right()) {
                return false;
            }
        }

        return true;
    }

    #[inline]
    fn iterate_over_entries<F>(
        &self,
        copy_context: &mut CopyContext<'arena, D>,
        header_len: usize,
        header_buffer: &mut [u8],
        constant_header_offset: usize,
        dynamic_header_offset: usize,
        cur_entry_ptr: &mut usize,
        datapath_callback: &mut F,
        callback_state: &mut D::CallbackEntryState,
    ) -> Result<usize>
    where
        F: FnMut(&D::DatapathMetadata, &mut D::CallbackEntryState) -> Result<()>,
    {
        self.serialize_bitmap(header_buffer, constant_header_offset);
        let mut cur_constant_offset =
            constant_header_offset + BITMAP_LENGTH_FIELD + Self::bitmap_length();

        let mut cur_dynamic_offset = dynamic_header_offset;
        let mut ret = 0;

        if self.get_bitmap_field(Self::LEFT_BITMAP_IDX, Self::LEFT_BITMAP_OFFSET) {
            write_size_and_offset(cur_constant_offset, 0, cur_dynamic_offset, header_buffer);
            ret += self.left.iterate_over_entries(
                copy_context,
                header_len,
                header_buffer,
                constant_header_offset,
                dynamic_header_offset,
                cur_entry_ptr,
                datapath_callback,
                callback_state,
            )?;

            cur_constant_offset += Tree4LCF::<D>::CONSTANT_HEADER_SIZE;
            cur_dynamic_offset += self.left.dynamic_header_size();
        }

        if self.get_bitmap_field(Self::RIGHT_BITMAP_IDX, Self::RIGHT_BITMAP_OFFSET) {
            write_size_and_offset(cur_constant_offset, 0, cur_dynamic_offset, header_buffer);
            ret += self.right.iterate_over_entries(
                copy_context,
                header_len,
                header_buffer,
                constant_header_offset,
                dynamic_header_offset,
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
        datapath: &mut D,
        header: &mut [u8],
        constant_header_offset: usize,
        dynamic_header_start: usize,
        copy_context: &mut CopyContext<'a, D>,
        zero_copy_scatter_gather_entries: &mut [D::DatapathMetadata],
        ds_offset: &mut usize,
    ) -> Result<()> {
        self.serialize_bitmap(header, constant_header_offset);
        let mut cur_constant_offset =
            constant_header_offset + BITMAP_LENGTH_FIELD + Self::bitmap_length();

        let mut cur_dynamic_offset = dynamic_header_start;
        let mut cur_sge_idx = 0;

        if self.get_bitmap_field(Self::LEFT_BITMAP_IDX, Self::LEFT_BITMAP_OFFSET) {
            write_size_and_offset(cur_constant_offset, 0, cur_dynamic_offset, header);
            self.left.inner_serialize(
                datapath,
                header,
                cur_dynamic_offset,
                cur_dynamic_offset + self.left.dynamic_header_start(),
                copy_context,
                &mut zero_copy_scatter_gather_entries
                    [cur_sge_idx..(cur_sge_idx + self.left.num_zero_copy_scatter_gather_entries())],
                ds_offset,
            )?;

            cur_sge_idx += self.left.num_zero_copy_scatter_gather_entries();
            cur_constant_offset += Tree4LCF::<D>::CONSTANT_HEADER_SIZE;
            cur_dynamic_offset += self.left.dynamic_header_size();
        }

        if self.get_bitmap_field(Self::RIGHT_BITMAP_IDX, Self::RIGHT_BITMAP_OFFSET) {
            write_size_and_offset(cur_constant_offset, 0, cur_dynamic_offset, header);
            self.right.inner_serialize(
                datapath,
                header,
                cur_dynamic_offset,
                cur_dynamic_offset + self.right.dynamic_header_start(),
                copy_context,
                &mut zero_copy_scatter_gather_entries[cur_sge_idx
                    ..(cur_sge_idx + self.right.num_zero_copy_scatter_gather_entries())],
                ds_offset,
            )?;
        }

        Ok(())
    }

    #[inline]
    fn inner_deserialize(
        &mut self,
        buffer: &D::DatapathMetadata,
        header_offset: usize,
        buffer_offset: usize,
        arena: &'arena bumpalo::Bump,
    ) -> Result<()> {
        let bitmap_size = self.deserialize_bitmap(buffer, header_offset, buffer_offset);
        let mut cur_constant_offset = header_offset + BITMAP_LENGTH_FIELD + bitmap_size;

        if self.get_bitmap_field(Self::LEFT_BITMAP_IDX, Self::LEFT_BITMAP_OFFSET) {
            self.left.inner_deserialize(
                buffer,
                read_size_and_offset::<D>(cur_constant_offset, buffer)?.1,
                buffer_offset,
                arena,
            )?;
            cur_constant_offset += Tree4LCF::<D>::CONSTANT_HEADER_SIZE;
        }

        if self.get_bitmap_field(Self::RIGHT_BITMAP_IDX, Self::RIGHT_BITMAP_OFFSET) {
            self.right.inner_deserialize(
                buffer,
                read_size_and_offset::<D>(cur_constant_offset, buffer)?.1,
                buffer_offset,
                arena,
            )?;
        }

        Ok(())
    }
}
