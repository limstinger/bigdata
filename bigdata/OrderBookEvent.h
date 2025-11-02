#pragma once

#include <string>
#include <vector>
#include <utility>
#include <cstdint>

// ====== 공통 이벤트 ======
struct OrderBookEvent {
    std::string exchange;   // "binance", "okx", "bybit"
    std::string symbol;     // "BTC-USD", ...
    double      event_ts;   // exchange가 준 시각 (sec)
    std::uint64_t seq;      // exchange 시퀀스

    // 간단히 bid/ask 1레벨만 예시로
    std::vector<std::pair<double, double>> bids;
    std::vector<std::pair<double, double>> asks;
};
