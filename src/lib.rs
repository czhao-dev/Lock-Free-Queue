//! `lfqueue` — a bounded, lock-free MPMC queue, benchmarked against a
//! mutex-protected baseline. See the README for the design rationale.

mod cache_padded;
mod mpmc_queue;
mod mutex_queue;

pub use mpmc_queue::MpmcQueue;
pub use mutex_queue::MutexQueue;

/// Common interface shared by [`MpmcQueue`] and [`MutexQueue`], used to
/// write tests and benchmarks generically over both implementations.
pub trait BoundedQueue<T> {
    fn with_capacity(capacity: usize) -> Self;
    fn push(&self, value: T) -> Result<(), T>;
    fn pop(&self) -> Option<T>;
    fn capacity(&self) -> usize;
}

impl<T> BoundedQueue<T> for MpmcQueue<T> {
    fn with_capacity(capacity: usize) -> Self {
        MpmcQueue::new(capacity)
    }
    fn push(&self, value: T) -> Result<(), T> {
        MpmcQueue::push(self, value)
    }
    fn pop(&self) -> Option<T> {
        MpmcQueue::pop(self)
    }
    fn capacity(&self) -> usize {
        MpmcQueue::capacity(self)
    }
}

impl<T> BoundedQueue<T> for MutexQueue<T> {
    fn with_capacity(capacity: usize) -> Self {
        MutexQueue::new(capacity)
    }
    fn push(&self, value: T) -> Result<(), T> {
        MutexQueue::push(self, value)
    }
    fn pop(&self) -> Option<T> {
        MutexQueue::pop(self)
    }
    fn capacity(&self) -> usize {
        MutexQueue::capacity(self)
    }
}
