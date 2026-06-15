#ifndef LFQUEUE_MUTEX_QUEUE_HPP
#define LFQUEUE_MUTEX_QUEUE_HPP

#include <cstddef>
#include <deque>
#include <mutex>
#include <stdexcept>
#include <utility>

namespace lfqueue {

template <typename T>
class MutexQueue {
public:
    explicit MutexQueue(std::size_t capacity)
        : capacity_(checked_capacity(capacity)) {}

    MutexQueue(const MutexQueue&) = delete;
    MutexQueue& operator=(const MutexQueue&) = delete;
    MutexQueue(MutexQueue&&) = delete;
    MutexQueue& operator=(MutexQueue&&) = delete;

    bool push(T value) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (queue_.size() == capacity_) {
            return false;
        }

        queue_.push_back(std::move(value));
        return true;
    }

    bool pop(T& out) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (queue_.empty()) {
            return false;
        }

        out = std::move(queue_.front());
        queue_.pop_front();
        return true;
    }

    std::size_t capacity() const noexcept {
        return capacity_;
    }

private:
    static std::size_t checked_capacity(std::size_t capacity) {
        if (capacity == 0) {
            throw std::invalid_argument("MutexQueue capacity must be non-zero");
        }
        return capacity;
    }

    const std::size_t capacity_;
    mutable std::mutex mutex_;
    std::deque<T> queue_;
};

} // namespace lfqueue

#endif // LFQUEUE_MUTEX_QUEUE_HPP
