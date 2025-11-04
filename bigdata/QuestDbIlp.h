// QuestDbIlp.h  (C++17 / Boost.Asio 필요)
#pragma once
#include <string>
#include <sstream>
#include <stdexcept>
#include <utility>
#include <vector>
#include <boost/asio.hpp>

// 매우 단순한 ILP TCP 클라이언트 (자동 재연결 + 배치 전송)
class QuestDbIlpClient {
public:
    QuestDbIlpClient(const std::string& host = "127.0.0.1", const std::string& port = "9009")
        : io_(), sock_(io_), resolver_(io_), host_(host), port_(port) {
        connect();
    }

    // 라인을 하나 추가 (개행 포함 X여도 됨; 내부에서 개행 붙임)
    void append_line(const std::string& line) {
        buf_ += line;
        if (!buf_.empty() && buf_.back() != '\n') buf_.push_back('\n');
        if (++count_ >= batch_count_ || buf_.size() >= batch_bytes_) {
            flush();
        }
    }

    // 강제 flush
    void flush() {
        if (buf_.empty()) return;
        ensure_connected();
        boost::asio::write(sock_, boost::asio::buffer(buf_));
        buf_.clear();
        count_ = 0;
    }

    // 종료 시 flush
    ~QuestDbIlpClient() {
        try { flush(); }
        catch (...) {}
        boost::system::error_code ec;
        sock_.close(ec);
    }

    // 튜닝(옵션)
    void set_batch(size_t max_bytes, int max_count) {
        batch_bytes_ = max_bytes;
        batch_count_ = max_count;
    }

private:
    void connect() {
        auto ep = resolver_.resolve(host_, port_);
        boost::asio::connect(sock_, ep);
    }
    void ensure_connected() {
        if (!sock_.is_open()) connect();
    }

    boost::asio::io_context io_;
    boost::asio::ip::tcp::socket sock_;
    boost::asio::ip::tcp::resolver resolver_;
    std::string host_, port_;

    std::string buf_;
    size_t batch_bytes_ = 64 * 1024; // 64KB
    int    batch_count_ = 500;       // 500라인
    int    count_ = 0;
};
