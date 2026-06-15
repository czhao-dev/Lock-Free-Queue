#include "lfqueue/mpmc_queue.hpp"
#include "lfqueue/mutex_queue.hpp"

#include <cassert>
#include <memory>
#include <stdexcept>

namespace {

template <template <typename> class Queue>
void test_empty_and_full() {
    Queue<int> queue(4);
    int value = -1;

    assert(queue.capacity() == 4);
    assert(!queue.pop(value));

    assert(queue.push(1));
    assert(queue.push(2));
    assert(queue.push(3));
    assert(queue.push(4));
    assert(!queue.push(5));

    assert(queue.pop(value));
    assert(value == 1);
    assert(queue.pop(value));
    assert(value == 2);
    assert(queue.pop(value));
    assert(value == 3);
    assert(queue.pop(value));
    assert(value == 4);
    assert(!queue.pop(value));
}

template <template <typename> class Queue>
void test_wraparound_order() {
    Queue<int> queue(2);
    int value = 0;

    assert(queue.push(10));
    assert(queue.push(20));
    assert(!queue.push(30));

    assert(queue.pop(value));
    assert(value == 10);

    assert(queue.push(30));

    assert(queue.pop(value));
    assert(value == 20);
    assert(queue.pop(value));
    assert(value == 30);
    assert(!queue.pop(value));
}

template <template <typename> class Queue>
void test_rejects_zero_capacity() {
    bool rejected_zero = false;
    try {
        Queue<int> queue(0);
    } catch (const std::invalid_argument&) {
        rejected_zero = true;
    }
    assert(rejected_zero);
}

void test_mpmc_constructor_validation() {
    bool rejected_non_power_of_two = false;
    try {
        lfqueue::MPMCQueue<int> queue(3);
    } catch (const std::invalid_argument&) {
        rejected_non_power_of_two = true;
    }
    assert(rejected_non_power_of_two);
}

template <template <typename> class Queue>
void test_move_only_values() {
    Queue<std::unique_ptr<int>> queue(2);
    std::unique_ptr<int> value;

    assert(queue.push(std::make_unique<int>(42)));
    assert(queue.pop(value));
    assert(value);
    assert(*value == 42);
    assert(!queue.pop(value));
}

template <template <typename> class Queue>
void test_basic_queue_behavior() {
    test_empty_and_full<Queue>();
    test_wraparound_order<Queue>();
    test_rejects_zero_capacity<Queue>();
    test_move_only_values<Queue>();
}

} // namespace

int main() {
    test_basic_queue_behavior<lfqueue::MPMCQueue>();
    test_basic_queue_behavior<lfqueue::MutexQueue>();
    test_mpmc_constructor_validation();
}
