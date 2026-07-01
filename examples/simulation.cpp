// fxmatch simulation demo.
//
// Spawns a FeedSimulator, a MatchingEngine, and a Reporter, runs for a
// configurable number of seconds, then prints per-symbol latency stats.
//
// Usage:
//   ./fxmatch_sim [--symbols SYM1,SYM2,...] [--secs N]
//
// Defaults: symbols=AAPL, secs=5

#include "fxmatch/clock.hpp"
#include "fxmatch/engine.hpp"
#include "fxmatch/feed.hpp"
#include "fxmatch/fill.hpp"
#include "fxmatch/mpmc_queue.hpp"
#include "fxmatch/order.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

// ---------------------------------------------------------------------------
// Argument parsing (no external dependencies)
// ---------------------------------------------------------------------------

struct Config {
    std::vector<std::string> symbol_strs = {"AAPL"};
    int secs = 5;
};

static Config parse_args(int argc, char* argv[]) {
    Config cfg;
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--symbols") == 0 && i + 1 < argc) {
            cfg.symbol_strs.clear();
            std::string s(argv[++i]);
            std::size_t start = 0, end;
            while ((end = s.find(',', start)) != std::string::npos) {
                cfg.symbol_strs.push_back(s.substr(start, end - start));
                start = end + 1;
            }
            cfg.symbol_strs.push_back(s.substr(start));
        } else if (std::strcmp(argv[i], "--secs") == 0 && i + 1 < argc) {
            cfg.secs = std::stoi(argv[++i]);
        }
    }
    return cfg;
}

// ---------------------------------------------------------------------------
// Reporter: accumulates fills and computes latency stats at shutdown
// ---------------------------------------------------------------------------

struct SymbolStats {
    uint64_t fill_count{0};
    std::vector<int64_t> latencies;
};

struct Reporter {
    fxmatch::MPMCQueue<fxmatch::Fill>& fill_rx;
    std::atomic<bool>& shutdown;

    std::unordered_map<fxmatch::Symbol,
                       SymbolStats,
                       fxmatch::SymbolHash> stats;
    std::mutex stats_mu;

    void run() {
        fxmatch::Fill fill;
        while (true) {
            if (fill_rx.pop(fill)) {
                int64_t lat = static_cast<int64_t>(fill.fill_ns)
                            - static_cast<int64_t>(fill.submit_ns);
                std::lock_guard<std::mutex> lk(stats_mu);
                auto& s = stats[fill.symbol];
                ++s.fill_count;
                s.latencies.push_back(lat > 0 ? lat : 0);
            } else if (shutdown.load(std::memory_order_relaxed)) {
                // Drain any remaining fills before exiting
                while (fill_rx.pop(fill)) {
                    int64_t lat = static_cast<int64_t>(fill.fill_ns)
                                - static_cast<int64_t>(fill.submit_ns);
                    auto& s = stats[fill.symbol];
                    ++s.fill_count;
                    s.latencies.push_back(lat > 0 ? lat : 0);
                }
                break;
            }
        }
    }

    void print(double elapsed_secs,
               const std::vector<fxmatch::Symbol>& symbols) {
        std::printf("\n=== fxmatch simulation (%.2fs) ===\n", elapsed_secs);
        std::printf("%-10s  %10s  %10s  %8s  %10s  %10s  %10s\n",
                    "Symbol", "Fills", "Fill/s",
                    "Fill%", "p50(ns)", "p99(ns)", "p999(ns)");

        uint64_t total_fills = 0;

        for (const auto& sym : symbols) {
            auto it = stats.find(sym);
            if (it == stats.end()) {
                std::printf("%-10s  (no fills)\n", sym.data());
                continue;
            }
            auto& s = it->second;
            total_fills += s.fill_count;

            std::sort(s.latencies.begin(), s.latencies.end());
            auto pct = [&](double p) -> int64_t {
                if (s.latencies.empty()) return 0;
                auto idx = static_cast<std::size_t>(
                    static_cast<double>(s.latencies.size()) * p);
                if (idx >= s.latencies.size()) idx = s.latencies.size() - 1;
                return s.latencies[idx];
            };

            char sym_str[9] = {};
            std::memcpy(sym_str, sym.data(), 8);
            // trim trailing spaces
            for (int k = 7; k >= 0 && sym_str[k] == ' '; --k) sym_str[k] = '\0';

            std::printf("%-10s  %10llu  %10.0f  %8.1f  %10lld  %10lld  %10lld\n",
                        sym_str,
                        static_cast<unsigned long long>(s.fill_count),
                        static_cast<double>(s.fill_count) / elapsed_secs,
                        0.0,  // fill% requires order count; see below
                        static_cast<long long>(pct(0.50)),
                        static_cast<long long>(pct(0.99)),
                        static_cast<long long>(pct(0.999)));
        }

        std::printf("%-10s  %10llu  %10.0f\n",
                    "Total",
                    static_cast<unsigned long long>(total_fills),
                    static_cast<double>(total_fills) / elapsed_secs);
        std::printf("Queue primitive: MPMCQueue (lock-free, Vyukov design, "
                    "64-byte padded head/tail)\n");
    }
};

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

int main(int argc, char* argv[]) {
    const Config cfg = parse_args(argc, argv);

    std::vector<fxmatch::Symbol> symbols;
    for (const auto& s : cfg.symbol_strs)
        symbols.push_back(fxmatch::make_symbol(s.c_str()));

    constexpr std::size_t kQueueCap = 65536;
    fxmatch::MPMCQueue<fxmatch::Order> order_q(kQueueCap);
    fxmatch::MPMCQueue<fxmatch::Fill>  fill_q(kQueueCap);
    std::atomic<bool> shutdown{false};

    fxmatch::MatchingEngine<fxmatch::MPMCQueue, fxmatch::MPMCQueue> engine(order_q, fill_q, shutdown);
    fxmatch::FeedSimulator  feed(order_q, symbols, 10000, shutdown, /*seed=*/42);
    Reporter reporter{fill_q, shutdown, {}, {}};

    std::thread feed_thread([&]{ feed.run(); });
    std::thread engine_thread([&]{ engine.run(); });
    std::thread reporter_thread([&]{ reporter.run(); });

    const auto t0 = std::chrono::steady_clock::now();
    std::this_thread::sleep_for(std::chrono::seconds(cfg.secs));
    shutdown.store(true, std::memory_order_relaxed);

    feed_thread.join();
    engine_thread.join();
    reporter_thread.join();

    const double elapsed = std::chrono::duration<double>(
        std::chrono::steady_clock::now() - t0).count();

    reporter.print(elapsed, symbols);
    return 0;
}
