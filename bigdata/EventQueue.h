#pragma once

#include <queue>
#include <mutex>
#include <condition_variable>
#include "OrderBookEvent.h"

// ====== simple MPSC queue ======
class EventQueue {
public:
    void push(const OrderBookEvent& ev) {
        std::lock_guard<std::mutex> lg(m_);
        q_.push(ev);
        cv_.notify_one();
    }

    bool pop(OrderBookEvent& out) {
        std::unique_lock<std::mutex> lk(m_);
        cv_.wait(lk, [&] { return !q_.empty() || stop_; });
        if (q_.empty()) return false;
        out = q_.front();
        q_.pop();
        return true;
    }

    void stop() {
        std::lock_guard<std::mutex> lg(m_);
        stop_ = true;
        cv_.notify_all();
    }

private:
    std::queue<OrderBookEvent> q_;
    std::mutex m_;
    std::condition_variable cv_;
    bool stop_ = false;
};
