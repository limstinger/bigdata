#pragma once
#include <map>
#include <mutex>
#include <string>
#include <vector>
#include <utility>
#include <functional>
#include "OrderBookEvent.h"

// 오더북(상위 N 추출, seq 역전 방지)
class LocalOrderBook {
public:
    LocalOrderBook() = default;
    LocalOrderBook(std::string ex, std::string sym)
        : exchange(std::move(ex)), symbol(std::move(sym)) {}

    void apply(const OrderBookEvent& ev) {
        std::lock_guard<std::mutex> lg(m_);
        if (ev.seq && ev.seq < last_seq) return; // seq 역전 방지
        for (auto& [p, q] : ev.bids) { if (q == 0) bids.erase(p); else bids[p] = q; }
        for (auto& [p, q] : ev.asks) { if (q == 0) asks.erase(p); else asks[p] = q; }
        last_seq = ev.seq;
    }

    std::vector<std::pair<double, double>> top_bids(int n = 20) const {
        std::vector<std::pair<double, double>> v; int c = 0;
        for (auto& kv : bids) { if (c++ >= n) break; v.push_back(kv); }
        return v;
    }
    std::vector<std::pair<double, double>> top_asks(int n = 20) const {
        std::vector<std::pair<double, double>> v; int c = 0;
        for (auto& kv : asks) { if (c++ >= n) break; v.push_back(kv); }
        return v;
    }

    std::string exchange, symbol;
    std::map<double, double, std::greater<double>> bids; // 내림차순
    std::map<double, double> asks;                      // 오름차순
    std::uint64_t last_seq = 0;

private:
    mutable std::mutex m_;
};
