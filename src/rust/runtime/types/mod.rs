// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

mod memory;
mod ops;
mod queue;

//==============================================================================
// Exports
//==============================================================================

pub use self::{
    memory::{
        datapath_buffer_t,
        datapath_metadata_t,
        datapath_recovery_info_t,
        demi_sgarray_t,
        demi_sgaseg_t,
        ofed_recovery_info_t,
        MempoolID,
        DEMI_SGARRAY_MAXLEN,
    },
    ops::{
        demi_accept_result_t,
        demi_opcode_t,
        demi_qr_value_t,
        demi_qresult_t,
    },
    queue::demi_qtoken_t,
};
