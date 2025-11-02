#include <iostream>
#include <thread>
#include <unordered_map>
#include <map>
#include <atomic>
#include <chrono>

#include "EventQueue.h"
#include "LocalOrderBook.h"
#include "OrderBookEvent.h"

// ====== 1) 거래소 스트리머 베이스 ======
class BaseStreamer {
public:
    BaseStreamer(std::string ex,
        std::unordered_map<std::string, std::string> symbolMap,
        EventQueue& out)
        : exchange_(std::move(ex)),
        symbolMap_(std::move(symbolMap)),
        out_(out)
    {}

    virtual ~BaseStreamer() = default;
    virtual void run() = 0;

protected:
    std::string exchange_;
    std::unordered_map<std::string, std::string> symbolMap_;
    EventQueue& out_;

    void publish(const OrderBookEvent& ev) {
        out_.push(ev);
    }
};

// ====== 각 거래소 더미 스트리머 ======
class BinanceStreamer : public BaseStreamer {
public:
    using BaseStreamer::BaseStreamer;

    void run() override {
        while (running_) {
            for (auto& kv : symbolMap_) {
                OrderBookEvent ev;
                ev.exchange = exchange_;
                ev.symbol = kv.first;
                ev.event_ts = now_sec();
                ev.seq = ++seq_counter_[ev.symbol];
                ev.bids = { {10000.0 + ev.seq, 0.5} };
                ev.asks = { {10000.5 + ev.seq, 0.4} };
                publish(ev);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
    }

    void stop() { running_ = false; }

private:
    std::atomic<bool> running_{ true };
    std::unordered_map<std::string, std::uint64_t> seq_counter_;

    static double now_sec() {
        using namespace std::chrono;
        return duration<double>(steady_clock::now().time_since_epoch()).count();
    }
};

class OKXStreamer : public BaseStreamer {
public:
    using BaseStreamer::BaseStreamer;

    void run() override {
        while (running_) {
            for (auto& kv : symbolMap_) {
                OrderBookEvent ev;
                ev.exchange = exchange_;
                ev.symbol = kv.first;
                ev.event_ts = now_sec();
                ev.seq = ++seq_counter_[ev.symbol];
                ev.bids = { {9000.0 + ev.seq, 0.3} };
                ev.asks = { {9000.5 + ev.seq, 0.25} };
                publish(ev);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1100));
        }
    }

    void stop() { running_ = false; }

private:
    std::atomic<bool> running_{ true };
    std::unordered_map<std::string, std::uint64_t> seq_counter_;

    static double now_sec() {
        using namespace std::chrono;
        return duration<double>(steady_clock::now().time_since_epoch()).count();
    }
};

class BybitStreamer : public BaseStreamer {
public:
    using BaseStreamer::BaseStreamer;

    void run() override {
        while (running_) {
            for (auto& kv : symbolMap_) {
                OrderBookEvent ev;
                ev.exchange = exchange_;
                ev.symbol = kv.first;
                ev.event_ts = now_sec();
                ev.seq = ++seq_counter_[ev.symbol];
                ev.bids = { {8000.0 + ev.seq, 0.8} };
                ev.asks = { {8000.5 + ev.seq, 0.7} };
                publish(ev);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1200));
        }
    }

    void stop() { running_ = false; }

private:
    std::atomic<bool> running_{ true };
    std::unordered_map<std::string, std::uint64_t> seq_counter_;

    static double now_sec() {
        using namespace std::chrono;
        return duration<double>(steady_clock::now().time_since_epoch()).count();
    }
};

// ====== 2) 오더북 소비 워커 ======
void orderbook_worker(EventQueue& q,
    std::map<std::string, LocalOrderBook>& books,
    std::atomic<bool>& running)
{
    while (running) {
        OrderBookEvent ev;
        if (!q.pop(ev)) break;

        std::string key = ev.exchange + ":" + ev.symbol;
        auto it = books.find(key);
        if (it == books.end()) {
            it = books.emplace(key, LocalOrderBook(ev.exchange, ev.symbol)).first;
        }
        it->second.apply(ev);
        it->second.print_top1();
    }
}

int main() {
    EventQueue q;

    // 우리 심볼 → 거래소 심볼
    std::unordered_map<std::string, std::string> binance_map = {
        {"BTC-USD", "btcusdt"},
        {"ETH-USD", "ethusdt"},
        {"SOL-USD", "solusdt"},
    };
    std::unordered_map<std::string, std::string> okx_map = {
        {"BTC-USD", "BTC-USDT"},
        {"ETH-USD", "ETH-USDT"},
        {"SOL-USD", "SOL-USDT"},
    };
    std::unordered_map<std::string, std::string> bybit_map = {
        {"BTC-USD", "BTCUSDT"},
        {"ETH-USD", "ETHUSDT"},
        {"SOL-USD", "SOLUSDT"},
    };

    BinanceStreamer binance("binance", binance_map, q);
    OKXStreamer     okx("okx", okx_map, q);
    BybitStreamer   bybit("bybit", bybit_map, q);

    std::map<std::string, LocalOrderBook> books;
    std::atomic<bool> running{ true };

    std::thread worker([&] {
        orderbook_worker(q, books, running);
        });

    std::thread t1([&] { binance.run(); });
    std::thread t2([&] { okx.run(); });
    std::thread t3([&] { bybit.run(); });

    std::this_thread::sleep_for(std::chrono::seconds(5));

    binance.stop();
    okx.stop();
    bybit.stop();
    running = false;
    q.stop();

    t1.join();
    t2.join();
    t3.join();
    worker.join();

    std::cout << "done\n";
    system("pause");
    return 0;
}
