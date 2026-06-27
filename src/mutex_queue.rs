use std::collections::VecDeque;
use std::sync::Mutex;

/// A bounded, mutex-protected queue used as the baseline to benchmark
/// `MpmcQueue` against.
pub struct MutexQueue<T> {
    capacity: usize,
    inner: Mutex<VecDeque<T>>,
}

impl<T> MutexQueue<T> {
    /// Creates a new queue with the given capacity.
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is zero.
    pub fn new(capacity: usize) -> Self {
        assert!(capacity != 0, "MutexQueue capacity must be non-zero");
        Self {
            capacity,
            inner: Mutex::new(VecDeque::with_capacity(capacity)),
        }
    }

    /// Attempts to push `value` onto the queue. Returns `Err(value)` if the
    /// queue is full.
    pub fn push(&self, value: T) -> Result<(), T> {
        let mut guard = self.inner.lock().unwrap();
        if guard.len() == self.capacity {
            return Err(value);
        }
        guard.push_back(value);
        Ok(())
    }

    /// Attempts to pop a value from the queue. Returns `None` if the queue
    /// is empty.
    pub fn pop(&self) -> Option<T> {
        self.inner.lock().unwrap().pop_front()
    }

    /// Returns the fixed capacity of the queue.
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}
