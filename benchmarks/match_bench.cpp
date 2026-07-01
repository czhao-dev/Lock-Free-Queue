// End-to-end order-to-fill latency benchmark.
//
// Measures the time from when an Order's timestamp_ns is set to when the
// MatchingEngine produces its Fill — the canonical HFT "tick-to-trade" metric.
// Run with: ./match_bench
//
// Prints CSV to stdout; redirect to benchmarks/results/match.csv.

#include "fxmatch/clock.hpp"
#include "fxmatch/engine.hpp"
#include "fxmatch/fill.hpp"
#include "fxmatch/mpmc_queue.hpp"
#include "fxmatch/mutex_queue.hpp"
#include "fxmatch/order.hpp"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <thread>
#include <vector>

// Use the same process-relative clock as the engine so that
// fill_ns - submit_ns gives a meaningful latency measurement.
static uint64_t bench_now_ns() { return fxmatch::now_ns(); }

// Pre-generate alternating Ask/Bid orders that will cross at price 10000,
// so every Ask is immediately filled by its paired Bid.
static std::vector<fxmatch::Order> make_orders(int n) {
    const auto sym = fxmatch::make_symbol("BENCH");
    std::vector<fxmatch::Order> orders;
    orders.reserve(static_cast<std::size_t>(n));
    for (int i = 0; i < n; ++i) {
        // Alternate Ask then Bid, both at price 10000, qty 1.
        // Each Ask rests until the following Bid crosses it → one fill per pair.
        fxmatch::Side side = (i % 2 == 0) ? fxmatch::Side::Ask : fxmatch::Side::Bid;
        orders.push_back(fxmatch::Order{
            static_cast<uint64_t>(i),
            sym,
            side,
            10000,
            1,
            0  // timestamp_ns filled in at push time
        });
    }
    return orders;
}

template <template <typename> class OQ, template <typename> class FQ>
void run_match_bench(const char* label, int n_orders, std::size_t q_capacity) {
    OQ<fxmatch::Order> order_q(q_capacity);
    FQ<fxmatch::Fill>  fill_q(q_capacity);
    std::atomic<bool>  shutdown{false};
    fxmatch::MatchingEngine<OQ, FQ> engine(order_q, fill_q, shutdown);

    const int n_fills = n_orders / 2;
    std::vector<int64_t> latencies;
    latencies.reserve(static_cast<std::size_t>(n_fills));

    auto orders = make_orders(n_orders);

    std::thread eng([&]{ engine.run(); });

    std::thread reporter([&]{
        fxmatch::Fill fill;
        while (static_cast<int>(latencies.size()) < n_fills) {
            if (fill_q.pop(fill)) {
                int64_t lat = static_cast<int64_t>(fill.fill_ns)
                            - static_cast<int64_t>(fill.submit_ns);
                latencies.push_back(lat > 0 ? lat : 0);
            }
        }
    });

    // Push orders with live timestamps so latency reflects real clock time.
    for (auto& order : orders) {
        order.timestamp_ns = bench_now_ns();
        while (!order_q.push(order)) {}
    }

    reporter.join();
    shutdown.store(true);
    eng.join();

    std::sort(latencies.begin(), latencies.end());
    auto pct = [&](double p) -> int64_t {
        auto idx = static_cast<std::size_t>(static_cast<double>(latencies.size()) * p);
        if (idx >= latencies.size()) idx = latencies.size() - 1;
        return latencies[idx];
    };

    std::printf("%s,%d,%lld,%lld,%lld\n",
                label, n_orders,
                static_cast<long long>(pct(0.50)),
                static_cast<long long>(pct(0.99)),
                static_cast<long long>(pct(0.999)));
}

int main() {
    std::puts("label,n_orders,p50_ns,p99_ns,p999_ns");

    // Lock-free MPMC pipeline
    run_match_bench<fxmatch::MPMCQueue, fxmatch::MPMCQueue>("mpmc",   100'000, 4096);
    run_match_bench<fxmatch::MPMCQueue, fxmatch::MPMCQueue>("mpmc", 1'000'000, 4096);

    // Mutex pipeline — baseline comparison
    run_match_bench<fxmatch::MutexQueue, fxmatch::MutexQueue>("mutex", 100'000, 4096);

    return 0;
}
