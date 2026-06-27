use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::cache_padded::CachePadded;

/// A slot in the ring buffer. `sequence` coordinates producer/consumer
/// handoff and encodes which lap around the ring the slot is on, which is
/// what avoids the ABA problem (see module docs / README).
struct Slot<T> {
    sequence: AtomicUsize,
    data: UnsafeCell<MaybeUninit<T>>,
}

/// A bounded, lock-free multi-producer multi-consumer queue.
///
/// Based on Dmitry Vyukov's bounded MPMC queue design: a fixed-size ring
/// buffer where each slot carries a sequence number instead of a 2-state
/// full/empty flag, so producers and consumers can claim slots via CAS
/// without ever blocking.
pub struct MpmcQueue<T> {
    capacity: usize,
    mask: usize,
    buffer: Box<[Slot<T>]>,
    head: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
}

// SAFETY: access to each slot's `data` is gated by the `sequence` atomic,
// which establishes acquire/release synchronization between the thread that
// writes a slot and the thread that later reads it. No two threads ever
// access the same slot's data concurrently.
unsafe impl<T: Send> Send for MpmcQueue<T> {}
unsafe impl<T: Send> Sync for MpmcQueue<T> {}

impl<T> MpmcQueue<T> {
    /// Creates a new queue with the given capacity, which must be a
    /// non-zero power of two (required for fast modular indexing via
    /// bitmasking).
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is zero or not a power of two.
    pub fn new(capacity: usize) -> Self {
        assert!(
            capacity != 0 && (capacity & (capacity - 1)) == 0,
            "MpmcQueue capacity must be a non-zero power of two"
        );

        let buffer: Box<[Slot<T>]> = (0..capacity)
            .map(|i| Slot {
                sequence: AtomicUsize::new(i),
                data: UnsafeCell::new(MaybeUninit::uninit()),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Self {
            capacity,
            mask: capacity - 1,
            buffer,
            head: CachePadded::new(AtomicUsize::new(0)),
            tail: CachePadded::new(AtomicUsize::new(0)),
        }
    }

    /// Attempts to push `value` onto the queue. Returns `Err(value)` if the
    /// queue is full, handing the value back to the caller rather than
    /// dropping it.
    pub fn push(&self, value: T) -> Result<(), T> {
        let mut pos = self.tail.value.load(Ordering::Relaxed);

        loop {
            let slot = &self.buffer[pos & self.mask];
            let seq = slot.sequence.load(Ordering::Acquire);
            let diff = seq as isize - pos as isize;

            if diff == 0 {
                if self
                    .tail
                    .value
                    .compare_exchange_weak(pos, pos + 1, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
                {
                    // SAFETY: this thread claimed slot `pos` via the
                    // successful CAS above, so it has exclusive write
                    // access until the release store below publishes it.
                    unsafe {
                        (*slot.data.get()).write(value);
                    }
                    slot.sequence.store(pos + 1, Ordering::Release);
                    return Ok(());
                }
            } else if diff < 0 {
                return Err(value);
            } else {
                pos = self.tail.value.load(Ordering::Relaxed);
            }
        }
    }

    /// Attempts to pop a value from the queue. Returns `None` if the queue
    /// is empty.
    pub fn pop(&self) -> Option<T> {
        let mut pos = self.head.value.load(Ordering::Relaxed);

        loop {
            let slot = &self.buffer[pos & self.mask];
            let seq = slot.sequence.load(Ordering::Acquire);
            let diff = seq as isize - (pos + 1) as isize;

            if diff == 0 {
                if self
                    .head
                    .value
                    .compare_exchange_weak(pos, pos + 1, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
                {
                    // SAFETY: the acquire load above observed the
                    // producer's release store, so the data write is
                    // visible; this thread claimed the slot via the
                    // successful CAS, so it has exclusive read access.
                    let value = unsafe { (*slot.data.get()).assume_init_read() };
                    slot.sequence.store(pos + self.capacity, Ordering::Release);
                    return Some(value);
                }
            } else if diff < 0 {
                return None;
            } else {
                pos = self.head.value.load(Ordering::Relaxed);
            }
        }
    }

    /// Returns the fixed capacity of the queue.
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

impl<T> Drop for MpmcQueue<T> {
    fn drop(&mut self) {
        // Destruction must not race with producers or consumers — by the
        // time `drop` runs, the caller has no other references, so plain
        // relaxed loads of head/tail are sufficient.
        let head = self.head.value.load(Ordering::Relaxed);
        let tail = self.tail.value.load(Ordering::Relaxed);

        let mut pos = head;
        while pos != tail {
            let slot = &self.buffer[pos & self.mask];
            let ready = pos + 1;
            if slot.sequence.load(Ordering::Relaxed) == ready {
                // SAFETY: a sequence number of `pos + 1` means a producer
                // published this slot and no consumer has taken it yet, so
                // it holds a valid, unread `T` that must be dropped.
                unsafe {
                    (*slot.data.get()).assume_init_drop();
                }
            }
            pos += 1;
        }
    }
}
