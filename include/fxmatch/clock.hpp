#ifndef FXMATCH_CLOCK_HPP
#define FXMATCH_CLOCK_HPP

#include <chrono>
#include <cstdint>

namespace fxmatch {

// Returns nanoseconds elapsed since the first call (process epoch).
// Using a process-relative epoch keeps values small and avoids overflow
// issues with raw time_since_epoch() on 32-bit targets.
inline uint64_t now_ns() noexcept {
    static const auto kEpoch = std::chrono::steady_clock::now();
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - kEpoch)
        .count());
}

} // namespace fxmatch

#endif // FXMATCH_CLOCK_HPP
