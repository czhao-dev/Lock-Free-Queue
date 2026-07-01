#ifndef FXMATCH_RNG_HPP
#define FXMATCH_RNG_HPP

#include <cstdint>

namespace fxmatch {

// Knuth's multiplicative LCG — fast, zero-allocation, portable.
// Good enough for feed simulation; not cryptographically secure.
struct Lcg64 {
    uint64_t state;

    explicit Lcg64(uint64_t seed) noexcept : state(seed | 1) {}

    uint64_t next() noexcept {
        state = state * 6364136223846793005ULL + 1442695040888963407ULL;
        return state;
    }

    uint64_t below(uint64_t n) noexcept { return next() % n; }
    bool     next_bool() noexcept       { return (next() & 1) == 0; }
};

} // namespace fxmatch

#endif // FXMATCH_RNG_HPP
