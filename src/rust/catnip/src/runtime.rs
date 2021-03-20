// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.
use crate::{
    protocols::{
        arp,
        ethernet2::MacAddress,
        tcp,
    },
    scheduler::{
        Operation,
        Scheduler,
        SchedulerHandle,
    },
    interop::dmtr_sgarray_t,
};
use rand::distributions::{
    Distribution,
    Standard,
};
use std::{
    fmt::Debug,
    future::Future,
    net::Ipv4Addr,
    time::{
        Duration,
        Instant,
    },
    ops::Deref,
};

pub trait RuntimeBuf: Clone + Debug + Deref<Target=[u8]> + Sized + Unpin {
    fn empty() -> Self;
    fn split(self, ix: usize) -> (Self, Self);

    fn from_sgarray(sga: &dmtr_sgarray_t) -> Self;
}

pub trait PacketBuf {
    fn compute_size(&self) -> usize;
    fn serialize(&self, buf: &mut [u8]);
}

pub trait Runtime: Clone + Unpin + 'static {
    type Buf: RuntimeBuf;

    fn advance_clock(&self, now: Instant);
    fn transmit(&self, pkt: impl PacketBuf);
    fn receive(&self) -> Option<Self::Buf>;

    fn local_link_addr(&self) -> MacAddress;
    fn local_ipv4_addr(&self) -> Ipv4Addr;
    fn arp_options(&self) -> arp::Options;
    fn tcp_options(&self) -> tcp::Options;

    type WaitFuture: Future<Output = ()>;
    fn wait(&self, duration: Duration) -> Self::WaitFuture;
    fn wait_until(&self, when: Instant) -> Self::WaitFuture;
    fn now(&self) -> Instant;

    fn rng_gen<T>(&self) -> T
    where
        Standard: Distribution<T>;
    fn rng_shuffle<T>(&self, slice: &mut [T]);

    fn spawn<F: Future<Output = ()> + 'static>(&self, future: F) -> SchedulerHandle;
    fn scheduler(&self) -> &Scheduler<Operation<Self>>;
}
