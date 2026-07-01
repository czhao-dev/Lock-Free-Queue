#ifndef FXMATCH_FEED_HPP
#define FXMATCH_FEED_HPP

#include <atomic>
#include <cstdint>
#include <thread>
#include <vector>
#include "fxmatch/clock.hpp"
#include "fxmatch/mpmc_queue.hpp"
#include "fxmatch/order.hpp"
#include "fxmatch/rng.hpp"

namespace fxmatch {

// Synthetic market data feed. Generates limit orders at maximum speed using a
// random walk around a per-symbol mid-price. Produces a mix of aggressive
// (crossing) and passive orders, yielding a realistic ~40-60% fill rate once
// the book has depth.
class FeedSimulator {
public:
    FeedSimulator(MPMCQueue<Order>& order_tx,
                  std::vector<Symbol> symbols,
                  uint64_t initial_mid_ticks,
                  std::atomic<bool>& shutdown,
                  uint64_t seed = 42) noexcept
        : order_tx_(order_tx),
          symbols_(std::move(symbols)),
          mids_(symbols_.size(), initial_mid_ticks),
          shutdown_(shutdown),
          rng_(seed) {}

    // Runs until shutdown is set. Spins on order_tx when the queue is full.
    void run() {
        uint64_t order_count = 0;

        while (!shutdown_.load(std::memory_order_relaxed)) {
            const std::size_t sym_idx = rng_.below(symbols_.size());
            const Symbol& sym = symbols_[sym_idx];

            // Gradual random walk: ±1 tick every 100 orders keeps mid from
            // drifting to zero or overflowing.
            if (order_count % 100 == 0) {
                mids_[sym_idx] += rng_.next_bool() ? 1ULL : static_cast<uint64_t>(-1LL);
            }

            const uint64_t mid = mids_[sym_idx];
            // offset ∈ [0, 4]: bids above mid cross resting asks; asks below
            // mid cross resting bids. A mix produces ~50% fill rate in steady state.
            const uint64_t offset = rng_.below(5);
            const Side side = rng_.next_bool() ? Side::Bid : Side::Ask;
            const uint64_t price = (side == Side::Bid)
                                       ? mid + offset
                                       : mid - offset;
            const uint64_t qty = 1 + rng_.below(10);

            Order order{next_id_++, sym, side, price, qty, now_ns()};

            while (!order_tx_.push(order)) {
                if (shutdown_.load(std::memory_order_relaxed)) return;
                std::this_thread::yield();
            }
            ++order_count;
        }
    }

private:
    MPMCQueue<Order>&  order_tx_;
    std::vector<Symbol> symbols_;
    std::vector<uint64_t> mids_;
    std::atomic<bool>& shutdown_;
    Lcg64 rng_;
    uint64_t next_id_{0};
};

} // namespace fxmatch

#endif // FXMATCH_FEED_HPP
