#pragma once
#include <string>
#include <vector>
#include <utility>
#include <cstdint>

struct OrderBookEvent {
    std::string exchange;    // "binance" | "okx" | "bybit"
    std::string symbol;      // "BTC-USD" | "ETH-USD" | "SOL-USD"
    std::string instance_id; // "A" or "B"

    double        event_ts = 0.0;  // 거래소 이벤트 시각(sec)  ★로그에는 이것만 기록
    double        recv_ts = 0.0;  // 로컬 수신 시각(sec)     (내부용)
    std::uint64_t seq = 0;    // 거래소 제공 시퀀스/버전

    // price, qty
    std::vector<std::pair<double, double>> bids;
    std::vector<std::pair<double, double>> asks;
};
