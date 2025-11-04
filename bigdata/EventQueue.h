#pragma once
#include <queue>
#include <mutex>
#include <condition_variable>
#include "OrderBookEvent.h"

// 단순 MPSC 큐
class EventQueue {
public:
    void push(OrderBookEvent&& ev) {
        std::lock_guard<std::mutex> lg(m_);
        q_.push(std::move(ev));
        cv_.notify_one();
    }
    bool pop(OrderBookEvent& ev) {
        std::unique_lock<std::mutex> lk(m_);
        cv_.wait(lk, [&] { return stop_ || !q_.empty(); });
        if (stop_ && q_.empty()) return false;
        ev = std::move(q_.front());
        q_.pop();
        return true;
    }
    void stop() {
        {
            std::lock_guard<std::mutex> lg(m_);
            stop_ = true;
        }
        cv_.notify_all();
    }
private:
    std::queue<OrderBookEvent> q_;
    std::mutex m_;
    std::condition_variable cv_;
    bool stop_ = false;
};
