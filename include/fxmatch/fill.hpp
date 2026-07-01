#ifndef FXMATCH_FILL_HPP
#define FXMATCH_FILL_HPP

#include <cstdint>
#include <type_traits>
#include "fxmatch/order.hpp"

namespace fxmatch {

// A Fill records a matched trade. latency = fill_ns - submit_ns.
struct Fill {
    uint64_t buy_order_id;
    uint64_t sell_order_id;
    Symbol   symbol;
    uint64_t price;
    uint64_t quantity;
    uint64_t submit_ns; // aggressor Order's timestamp_ns
    uint64_t fill_ns;   // steady_clock ns when the match was produced
};

static_assert(std::is_nothrow_move_constructible<Fill>::value, "");
static_assert(std::is_nothrow_move_assignable<Fill>::value, "");

} // namespace fxmatch

#endif // FXMATCH_FILL_HPP
