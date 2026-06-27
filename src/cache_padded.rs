/// Pads a value so it occupies an entire 64-byte cache line.
///
/// Used to keep `head` and `tail` on separate cache lines: without this,
/// every producer update to `tail` would invalidate the cache line for
/// consumers reading `head` and vice versa (false sharing).
#[repr(align(64))]
pub(crate) struct CachePadded<T> {
    pub(crate) value: T,
}

impl<T> CachePadded<T> {
    pub(crate) const fn new(value: T) -> Self {
        Self { value }
    }
}

const _: () = assert!(std::mem::size_of::<CachePadded<usize>>() >= 64);
