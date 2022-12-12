// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

//==============================================================================
// Imports
//==============================================================================

use crate::{
    cornflakes::{
        CopyContext,
        ObjEnum,
    },
    runtime::{
        fail::Fail,
        types::datapath_metadata_t,
    },
};
//==============================================================================
// Structures
//==============================================================================

#[derive(Debug, Clone)]
pub struct CornflakesObj {
    copy_context: Vec<datapath_metadata_t>,
    obj: ObjEnum,
    start_offset: usize,
    reference_len: usize,
    pkt_timestamp: u64,
    flow_id: u64,
}

//==============================================================================
// Associate Functions
//==============================================================================
impl CornflakesObj {
    pub fn new(object: ObjEnum, copy_context: CopyContext, timestamp: u64, flow_id: u64) -> Self {
        let total_data_len = object.total_length(&copy_context);
        debug!("Calculated object size as {}", total_data_len);
        CornflakesObj {
            obj: object,
            copy_context: copy_context.to_metadata_vec(),
            start_offset: 0,
            reference_len: total_data_len + 32,
            pkt_timestamp: timestamp,
            flow_id,
        }
    }

    pub fn offset(&self) -> usize {
        self.start_offset
    }

    pub fn get_timestamp(&self) -> u64 {
        self.pkt_timestamp
    }

    pub fn get_flow_id(&self) -> u64 {
        self.flow_id
    }

    pub fn len(&self) -> usize {
        self.reference_len
    }

    pub fn trim(&mut self, nbytes: usize) {
        self.start_offset += nbytes;
        self.reference_len -= nbytes;
    }

    pub fn adjust(&mut self, nbytes: usize) {
        self.reference_len -= nbytes;
    }

    pub fn num_segments_total(&self, with_header: bool) -> usize {
        self.obj.num_segments_total(
            with_header,
            &self.copy_context,
            self.start_offset,
            self.reference_len,
            32,
        )
    }

    pub fn write_header(&self, mut_header_slice: &mut [u8]) -> usize {
        self.obj.write_header(
            mut_header_slice,
            &self.copy_context,
            self.start_offset,
            self.reference_len,
            32,
        )
    }

    pub fn iterate_over_entries_with_callback<F, C>(&self, callback: &mut F, callback_state: &mut C)
    where
        F: FnMut(datapath_metadata_t, &mut C) -> Result<(), Fail>,
    {
        self.obj.iterate_over_entries_with_callback(
            &self.copy_context,
            self.start_offset,
            self.reference_len,
            32,
            callback,
            callback_state,
        );
    }
}
