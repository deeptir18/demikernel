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
    full_header_size: usize,
    copy_context: Vec<datapath_metadata_t>,
    obj: ObjEnum,
    start_offset: usize,
    total_data_len: usize,
}

//==============================================================================
// Associate Functions
//==============================================================================
impl CornflakesObj {
    pub fn new(object: ObjEnum, copy_context: CopyContext) -> Self {
        let header_size = object.total_header_size();
        let total_data_len = object.total_length(&copy_context);
        CornflakesObj {
            full_header_size: header_size,
            obj: object,
            copy_context: copy_context.to_metadata_vec(),
            start_offset: 0,
            total_data_len,
        }
    }

    pub fn len(&self) -> usize {
        self.total_data_len - self.start_offset
    }

    pub fn trim(&mut self, nbytes: usize) {
        self.start_offset += nbytes;
    }

    pub fn adjust(&mut self, nbytes: usize) {
        self.total_data_len -= nbytes;
    }
}
