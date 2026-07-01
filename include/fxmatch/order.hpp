#ifndef FXMATCH_ORDER_HPP
#define FXMATCH_ORDER_HPP

#include <array>
#include <cstdint>
#include <cstring>
#include <type_traits>

namespace fxmatch {

// 8-byte fixed-size ticker: trivially copyable, fits in a register pair.
// Padded with spaces (' ') — e.g. "AAPL    ".
using Symbol = std::array<char, 8>;

inline Symbol make_symbol(const char* s) noexcept {
    Symbol sym{};
    sym.fill(' ');
    for (std::size_t i = 0; i < 8 && s[i] != '\0'; ++i)
        sym[i] = s[i];
    return sym;
}

enum class Side : uint8_t { Bid, Ask };

// All fields are primitive or fixed-size — Order is trivially copyable and
// passes through the lock-free queue with zero heap allocation.
struct Order {
    uint64_t id;
    Symbol   symbol;
    Side     side;
    uint64_t price;        // integer ticks (e.g. cents); never float — float
                           // arithmetic is non-associative and breaks price-
                           // time priority determinism under concurrent access
    uint64_t quantity;
    uint64_t timestamp_ns; // nanoseconds since process epoch (see clock.hpp)
};

static_assert(std::is_nothrow_move_constructible<Order>::value, "");
static_assert(std::is_nothrow_move_assignable<Order>::value, "");

} // namespace fxmatch

#endif // FXMATCH_ORDER_HPP
