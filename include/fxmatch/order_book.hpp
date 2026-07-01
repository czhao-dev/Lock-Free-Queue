#ifndef FXMATCH_ORDER_BOOK_HPP
#define FXMATCH_ORDER_BOOK_HPP

#include <algorithm>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <optional>
#include <vector>
#include "fxmatch/order.hpp"
#include "fxmatch/fill.hpp"

namespace fxmatch {

// Per-symbol limit order book with price-time priority (FIFO within a level).
//
// This class is NOT thread-safe. It is owned entirely by the MatchingEngine
// thread — thread confinement means no locking is required on the book itself.
// The MPMCQueue at the pipeline boundary absorbs all concurrency; the book
// stays simple and cache-friendly.
class OrderBook {
public:
    explicit OrderBook(Symbol symbol) : symbol_(symbol) {}

    // Match an incoming order against resting liquidity. Returns all fills
    // produced. Any unfilled remainder is posted to the book (passive).
    //
    // fill_ns: steady_clock timestamp to record on each Fill (set by caller
    //          so the engine can batch one clock read per order rather than
    //          one per fill).
    std::vector<Fill> match_order(Order incoming, uint64_t fill_ns) {
        std::vector<Fill> fills;

        if (incoming.side == Side::Bid) {
            // Aggressor buy: match against asks from lowest price upward.
            auto it = asks_.begin();
            while (it != asks_.end()
                   && it->first <= incoming.price
                   && incoming.quantity > 0)
            {
                auto& level = it->second;
                while (!level.empty() && incoming.quantity > 0) {
                    Order& resting = level.front();
                    const uint64_t qty = std::min(incoming.quantity, resting.quantity);

                    fills.push_back(Fill{
                        incoming.id,
                        resting.id,
                        symbol_,
                        it->first,  // fill price = resting ask price
                        qty,
                        incoming.timestamp_ns,
                        fill_ns
                    });

                    incoming.quantity -= qty;
                    resting.quantity  -= qty;
                    if (resting.quantity == 0)
                        level.pop_front();
                }
                it = level.empty() ? asks_.erase(it) : std::next(it);
            }
        } else {
            // Aggressor sell: match against bids from highest price downward.
            auto it = bids_.begin();
            while (it != bids_.end()
                   && it->first >= incoming.price
                   && incoming.quantity > 0)
            {
                auto& level = it->second;
                while (!level.empty() && incoming.quantity > 0) {
                    Order& resting = level.front();
                    const uint64_t qty = std::min(incoming.quantity, resting.quantity);

                    fills.push_back(Fill{
                        resting.id,
                        incoming.id,
                        symbol_,
                        it->first,  // fill price = resting bid price
                        qty,
                        incoming.timestamp_ns,
                        fill_ns
                    });

                    incoming.quantity -= qty;
                    resting.quantity  -= qty;
                    if (resting.quantity == 0)
                        level.pop_front();
                }
                it = level.empty() ? bids_.erase(it) : std::next(it);
            }
        }

        // Post any unfilled remainder to the passive side of the book.
        if (incoming.quantity > 0) {
            if (incoming.side == Side::Bid)
                bids_[incoming.price].push_back(incoming);
            else
                asks_[incoming.price].push_back(incoming);
        }

        return fills;
    }

    std::optional<uint64_t> best_bid() const {
        if (bids_.empty()) return std::nullopt;
        return bids_.begin()->first;
    }

    std::optional<uint64_t> best_ask() const {
        if (asks_.empty()) return std::nullopt;
        return asks_.begin()->first;
    }

    std::size_t bid_depth() const noexcept { return bids_.size(); }
    std::size_t ask_depth() const noexcept { return asks_.size(); }

private:
    Symbol symbol_;

    // Bids: highest price first (std::greater keeps best bid at begin()).
    std::map<uint64_t, std::deque<Order>, std::greater<uint64_t>> bids_;

    // Asks: lowest price first (default std::less keeps best ask at begin()).
    std::map<uint64_t, std::deque<Order>> asks_;
};

} // namespace fxmatch

#endif // FXMATCH_ORDER_BOOK_HPP
