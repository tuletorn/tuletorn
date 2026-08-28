//! Ring, registered buffers, and the `user_data` encoding.
//!
//! Everything here exists to keep the hot path free of syscalls and
//! allocations. Buffers are registered once so the kernel never has to pin
//! pages per operation; SQEs accumulate and are handed over in one
//! `io_uring_enter` per loop turn; and a completion identifies its connection
//! by index rather than by a heap pointer.

use io_uring::{IoUring, squeue};
use std::io;

/// Operation kind, carried in the top byte of `user_data`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Op {
    Accept = 1,
    ClientRead = 2,
    ClientWrite = 3,
    UpstreamConnect = 4,
    UpstreamWrite = 5,
    UpstreamRead = 6,
    Close = 7,
}

impl Op {
    fn from_u8(v: u8) -> Option<Self> {
        Some(match v {
            1 => Op::Accept,
            2 => Op::ClientRead,
            3 => Op::ClientWrite,
            4 => Op::UpstreamConnect,
            5 => Op::UpstreamWrite,
            6 => Op::UpstreamRead,
            7 => Op::Close,
            _ => return None,
        })
    }
}

/// `user_data` layout: `[op:8][generation:24][index:32]`.
///
/// The generation is what makes a late completion safe: a slot reused by a new
/// connection carries a different generation, so a CQE from the previous
/// occupant is recognised as stale and dropped rather than applied to whoever
/// holds the slot now.
#[must_use]
pub fn pack(op: Op, generation: u32, index: u32) -> u64 {
    ((op as u64) << 56) | (((generation as u64) & 0xff_ffff) << 32) | (index as u64)
}

#[must_use]
pub fn unpack(user_data: u64) -> Option<(Op, u32, u32)> {
    let op = Op::from_u8((user_data >> 56) as u8)?;
    let generation = ((user_data >> 32) & 0xff_ffff) as u32;
    let index = user_data as u32;
    Some((op, generation, index))
}

/// A pool of pre-registered, fixed-size buffers.
///
/// Registration is the point: `ReadFixed`/`WriteFixed` against a registered
/// buffer skips the per-operation page pinning that a plain `Read`/`Write`
/// pays, and the buffer a response is read into is the same one it is written
/// out of, so forwarding costs no copy at all.
pub struct BufferPool {
    memory: Vec<u8>,
    buf_size: usize,
    free: Vec<u16>,
}

impl BufferPool {
    pub fn new(count: usize, buf_size: usize) -> io::Result<Self> {
        if count == 0 || count > u16::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "buffer count must be in 1..=65535",
            ));
        }
        let mut memory = vec![0u8; count * buf_size];
        // Fault the pages in now rather than during the first burst of traffic.
        for page in memory.iter_mut().step_by(4096) {
            *page = 0;
        }
        let free = (0..count as u16).rev().collect();
        Ok(Self {
            memory,
            buf_size,
            free,
        })
    }

    pub fn iovecs(&mut self) -> Vec<libc::iovec> {
        let size = self.buf_size;
        let count = self.memory.len() / size;
        let base = self.memory.as_mut_ptr();
        (0..count)
            .map(|i| libc::iovec {
                // SAFETY: `i * size` is in bounds for all `i < count`.
                iov_base: unsafe { base.add(i * size) }.cast(),
                iov_len: size,
            })
            .collect()
    }

    #[must_use]
    pub fn buf_size(&self) -> usize {
        self.buf_size
    }

    pub fn alloc(&mut self) -> Option<u16> {
        self.free.pop()
    }

    pub fn release(&mut self, index: u16) {
        self.free.push(index);
    }

    #[must_use]
    pub fn ptr(&mut self, index: u16) -> *mut u8 {
        // SAFETY: indices come from `alloc`, so they address a real slot.
        unsafe { self.memory.as_mut_ptr().add(index as usize * self.buf_size) }
    }

    #[must_use]
    pub fn slice(&self, index: u16, offset: usize, len: usize) -> &[u8] {
        let start = index as usize * self.buf_size + offset;
        &self.memory[start..start + len]
    }
}

/// The ring plus the count of SQEs waiting to be handed to the kernel.
pub struct Ring {
    pub io: IoUring,
    queued: u32,
}

impl Ring {
    pub fn new(entries: u32, single_issuer: bool) -> io::Result<Self> {
        let mut builder = IoUring::builder();
        if single_issuer {
            // One thread owns this ring for its whole life, so the kernel can
            // skip its cross-issuer synchronisation.
            builder.setup_single_issuer();
        }
        // Completions are only ever reaped by this same thread between
        // `submit_and_wait` calls, so the kernel need not send an IPI to
        // announce them.
        builder.setup_coop_taskrun();
        Ok(Self {
            io: builder.build(entries)?,
            queued: 0,
        })
    }

    /// Queue an SQE without entering the kernel.
    pub fn push(&mut self, entry: squeue::Entry) -> io::Result<()> {
        // SAFETY: every buffer an SQE points at is owned by the worker's
        // buffer pool and is not released until the matching CQE is seen.
        if unsafe { self.io.submission().push(&entry) }.is_err() {
            // The queue is full, so make room and retry once.
            self.submit_and_wait(0)?;
            // SAFETY: as above.
            unsafe { self.io.submission().push(&entry) }
                .map_err(|_| io::Error::other("io_uring submission queue full"))?;
        }
        self.queued += 1;
        Ok(())
    }

    /// Hand every queued SQE to the kernel and optionally block for completions.
    ///
    /// This is the only syscall on the hot path, and it covers the whole batch
    /// of operations the previous loop turn produced.
    pub fn submit_and_wait(&mut self, want: usize) -> io::Result<u32> {
        self.io.submission().sync();
        let submitted = self.io.submitter().submit_and_wait(want)? as u32;
        self.queued = self.queued.saturating_sub(submitted);
        Ok(submitted)
    }
}
