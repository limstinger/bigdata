// ===== 안정 빌드 매크로(컴파일러 독립) =====
#define BOOST_ASIO_DISABLE_STD_SOURCE_LOCATION
#define BOOST_ASIO_DISABLE_BOOST_SOURCE_LOCATION

#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include <cmath>

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <nlohmann/json.hpp>

#include "OrderBookEvent.h"
#include "EventQueue.h"
#include "LocalOrderBook.h"
#include "QuestDbIlp.h"

using json = nlohmann::json;
using ordered_json = nlohmann::ordered_json;
namespace asio = boost::asio;
namespace ssl = asio::ssl;
namespace beast = boost::beast;
namespace ws = beast::websocket;
namespace fs = std::filesystem;

// ========= 유틸 =========
static inline double now_sec() {
    using namespace std::chrono;
    return duration<double>(system_clock::now().time_since_epoch()).count();
}
static inline std::string ymd(double ts) {
    std::time_t t = (std::time_t)ts; char b[16];
#if defined(_WIN32)
    std::tm tm_buf; localtime_s(&tm_buf, &t);
    std::strftime(b, sizeof(b), "%Y-%m-%d", &tm_buf);
#else
    std::tm tm_buf; localtime_r(&t, &tm_buf);
    std::strftime(b, sizeof(b), "%Y-%m-%d", &tm_buf);
#endif
    return b;
}

static ssl::context make_tls_ctx() {
    ssl::context ctx(ssl::context::tls_client);
    ctx.set_default_verify_paths();
    // 실행 폴더에 있는 cacert.pem 우선 사용
    if (fs::exists("cacert.pem")) {
        try { ctx.load_verify_file("cacert.pem"); }
        catch (...) {}
    }
    ctx.set_verify_mode(ssl::verify_peer);
    return ctx;
}

// ========= 저장 경로/JSON =========
static fs::path devdb_path(const OrderBookEvent& ev) {
    fs::path root = fs::path("data") / "devdb" / ymd(ev.event_ts) / ev.exchange / ev.symbol;
    fs::create_directories(root);
    return root / (ev.instance_id + ".jsonl"); // ★ A/B 분리 저장
}
static ordered_json to_json(const OrderBookEvent& ev, const LocalOrderBook& book, const json& raw) {
    ordered_json depth;
    depth["last_seq"] = book.last_seq;
    depth["bids"] = json::array();
    for (auto& b : book.top_bids(20)) depth["bids"].push_back({ b.first,b.second });
    depth["asks"] = json::array();
    for (auto& a : book.top_asks(20)) depth["asks"].push_back({ a.first,a.second });

    ordered_json j;
    j["exchange"] = ev.exchange;
    j["instance_id"] = ev.instance_id;
    j["symbol"] = ev.symbol;
    j["event_ts"] = ev.event_ts; // ★오직 event_ts만 기록
    j["seq"] = ev.seq;
    j["depth"] = depth;
    j["raw"] = raw; // 원본 페이로드(디버깅용)
    return j;
}

// ========= 저장 워커 =========
static std::atomic<bool> running{ true };
static void on_sig(int) { running.store(false); }

static void store_worker(EventQueue& q,
    std::unordered_map<std::string, std::unique_ptr<LocalOrderBook>>& books) {
    fs::create_directories("data");
    std::ofstream all(fs::path("data") / "orderbook_stream.jsonl", std::ios::app);

    QuestDbIlpClient ilp("127.0.0.1", "9009");
    ilp.set_batch(64 * 1024, 500);  // 선택: 배치 크기 조정

    std::unordered_map<std::string, std::unique_ptr<std::ofstream>> cache;

    while (running.load()) {
        OrderBookEvent ev;
        if (!q.pop(ev)) break;

        // 로컬 오더북 갱신
        auto key = ev.exchange + ":" + ev.symbol + ":" + ev.instance_id;
        if (!books.count(key)) books[key] = std::make_unique<LocalOrderBook>(ev.exchange, ev.symbol);
        books[key]->apply(ev);

        // ===== ILP 라인 빌드 & 전송 =====
        auto bids = books[key]->top_bids(1);
        auto asks = books[key]->top_asks(1);

        double bb = bids.empty() ? 0.0 : bids[0].first;
        double bq = bids.empty() ? 0.0 : bids[0].second;
        double ba = asks.empty() ? 0.0 : asks[0].first;
        double aq = asks.empty() ? 0.0 : asks[0].second;

        auto to_ns = [](double secs) -> long long {
            return static_cast<long long>(secs * 1'000'000'000LL);
            };

        // measurement: orderbook
        // tags: exchange, symbol, instance
        // fields: seq, best_bid, best_ask, bid_qty, ask_qty
        std::ostringstream ilp_line;
        ilp_line << "orderbook"
            << ",exchange=" << ev.exchange
            << ",symbol=" << ev.symbol
            << ",instance=" << ev.instance_id
            << " seq=" << ev.seq << "i"
            << ",best_bid=" << bb
            << ",best_ask=" << ba
            << ",bid_qty=" << bq
            << ",ask_qty=" << aq
            << " " << to_ns(ev.event_ts);

        try {
            ilp.append_line(ilp_line.str());   // 배치에 쌓음 (임계치 도달 시 자동 flush)
        }
        catch (const std::exception& e) {
            // 네트워크 일시 장애 시 워커 멈추지 않도록 무시/로그
            std::cerr << "[ilp] " << e.what() << "\n";
        }



        // 보기 좋은 JSON
        auto j = to_json(ev, *books[key], json::object());
        all << j.dump() << '\n'; all.flush();

        auto p = devdb_path(ev);
        auto ps = p.string();
        if (!cache.count(ps)) {
            fs::create_directories(p.parent_path());
            cache[ps] = std::make_unique<std::ofstream>(p, std::ios::app);
        }
        (*cache[ps]) << j.dump() << '\n'; cache[ps]->flush();
    }
}

// ========= 공통 WSS 클라이언트 =========
class WssClient {
public:
    WssClient(asio::io_context& io, ssl::context& ctx,
        std::string host, std::string port, std::string target)
        : resolver_(io)
        , stream_(io, ctx)                         // 먼저 생성되고
        , ws_(std::move(stream_))                  // ✅ 이동해서 ws_를 만듭니다
        , host_(std::move(host))
        , port_(std::move(port))
        , target_(std::move(target)) {}

    void open() {
        auto res = resolver_.resolve(host_, port_);

        // ✅ ws_.next_layer() 로 ssl_stream 참조를 얻습니다
        auto& ssl_stream = ws_.next_layer();

        // TCP connect
        beast::get_lowest_layer(ssl_stream).connect(res);

        // SNI
        if (!SSL_set_tlsext_host_name(ssl_stream.native_handle(), host_.c_str())) {
            beast::error_code ec{ static_cast<int>(::ERR_get_error()),
                                  asio::error::get_ssl_category() };
            throw beast::system_error{ ec };
        }

        // TLS handshake
        ssl_stream.handshake(ssl::stream_base::client);

        // WebSocket handshake
        ws_.set_option(ws::stream_base::timeout::suggested(beast::role_type::client));
        ws_.handshake(host_, target_);
    }

    void send(const std::string& s) { ws_.write(asio::buffer(s)); }
    std::string read_text() {
        beast::flat_buffer buf; ws_.read(buf);
        return beast::buffers_to_string(buf.data());
    }
    void close() { beast::error_code ec; ws_.close(ws::close_code::normal, ec); }

private:
    asio::ip::tcp::resolver resolver_;
    beast::ssl_stream<beast::tcp_stream> stream_;                 // 이동 원본
    ws::stream<beast::ssl_stream<beast::tcp_stream>> ws_;         // 실제 사용
    std::string host_, port_, target_;
};


// ========= 심볼 맵 (우리 → 거래소) =========
struct SymbolMaps {
    // Binance /stream?streams=btcusdt@depth@100ms/...
    std::map<std::string, std::string> binance{
        {"BTC-USD","btcusdt"}, {"ETH-USD","ethusdt"}, {"SOL-USD","solusdt"}
    };
    // OKX v5 public books5: instId = BTC-USDT
    std::map<std::string, std::string> okx{
        {"BTC-USD","BTC-USDT"}, {"ETH-USD","ETH-USDT"}, {"SOL-USD","SOL-USDT"}
    };
    // Bybit spot v5: orderbook.50.BTCUSDT
    std::map<std::string, std::string> bybit{
        {"BTC-USD","BTCUSDT"}, {"ETH-USD","ETHUSDT"}, {"SOL-USD","SOLUSDT"}
    };
};

// --- [ADD] OKX용 단조 seq 합성 유틸 (키별: exchange:symbol:instance) ---
struct MonoSeq {
    uint64_t last_ms = 0;
    uint16_t sub = 0;
};
static std::mutex g_seq_m;
static std::unordered_map<std::string, MonoSeq> g_seq_reg;

// ts_ms가 같아도 항상 증가하도록 하위 12비트 서브카운터 부여
static uint64_t compose_monotonic_seq(const std::string& key, uint64_t ts_ms) {
    std::lock_guard<std::mutex> lk(g_seq_m);
    auto& s = g_seq_reg[key];
    if (ts_ms < s.last_ms) {
        // 네트워크 지터로 과거 프레임: 안전하게 드랍 신호(0) 리턴
        return 0;
    }
    if (ts_ms == s.last_ms) {
        if (++s.sub == 0) s.sub = 1; // (이론상) 오버플로 방지
    }
    else {
        s.last_ms = ts_ms;
        s.sub = 1;
    }
    return (ts_ms << 12) | s.sub; // 상위=ms, 하위=0..4095
}





// ========= 각 거래소 러너 =========
static void run_binance(asio::io_context& io, ssl::context& tls, const std::string& inst,
    const std::map<std::string, std::string>& symmap, EventQueue& out) {
    // 하나의 커넥션에 멀티 스트림
    std::ostringstream oss; bool first = true;
    for (auto& kv : symmap) { if (!first) oss << "/"; first = false; oss << kv.second << "@depth@100ms"; }
    std::string target = "/stream?streams=" + oss.str();

    while (running.load()) {
        try {
            WssClient c(io, tls, "stream.binance.com", "9443", target);
            c.open();
            for (;;) {
                auto s = c.read_text();
                double recv = now_sec();
                auto j = json::parse(s, nullptr, false);
                if (j.is_discarded() || !j.contains("data")) continue;
                const auto& d = j["data"];
                if (!d.contains("s") || !d.contains("E")) continue;

                std::string venue = d["s"].get<std::string>();         // "BTCUSDT"
                std::string venue_l = venue;
                std::transform(venue_l.begin(), venue_l.end(), venue_l.begin(),
                    [](unsigned char c) { return std::tolower(c); }); // "btcusdt"

                std::string unified;
                for (auto& kv : symmap) {
                    if (kv.second == venue_l) { // symmap 값이 소문자이므로 소문자로 비교
                        unified = kv.first;
                        break;
                    }
                }
                if (unified.empty()) {
                    // 디버깅용으로 한 줄 남겨도 좋아요:
                    // std::cerr << "[binance] unmatched symbol: " << venue << "\n";
                    continue;
                }

                OrderBookEvent ev;
                ev.exchange = "binance";
                ev.instance_id = inst;
                ev.symbol = unified;
                ev.seq = d.value("u", 0ULL);
                ev.event_ts = d["E"].get<double>() / 1000.0; // ms → s
                ev.recv_ts = recv;

                if (d.contains("b")) for (auto& x : d["b"]) if (x.size() >= 2)
                    ev.bids.emplace_back(std::stod(x[0].get<std::string>()), std::stod(x[1].get<std::string>()));
                if (d.contains("a")) for (auto& x : d["a"]) if (x.size() >= 2)
                    ev.asks.emplace_back(std::stod(x[0].get<std::string>()), std::stod(x[1].get<std::string>()));

                out.push(std::move(ev));
            }
        }
        catch (const std::exception& e) {
            std::cerr << "[binance] " << e.what() << " → reconnect in 3s\n";
            std::this_thread::sleep_for(std::chrono::seconds(3));
        }
    }
}

// === OKX 러너: seq 없으면 ts(ms)로 단조 증가 seq 합성 ===
static void run_okx(asio::io_context& io, ssl::context& tls, const std::string& inst,
    const std::map<std::string, std::string>& symmap, EventQueue& out) {

    // 안전한 ms 파서 (string/int/float 모두 처리)
    auto parse_ms = [](const json& x) -> double {
        try {
            if (x.is_string())         return std::stod(x.get<std::string>());
            if (x.is_number_integer()) return static_cast<double>(x.get<long long>());
            if (x.is_number_float())   return x.get<double>();
        }
        catch (...) {}
        return 0.0;
        };

    while (running.load()) {
        try {
            // 포트가 막혀 있으면 "443"으로 바꿔 테스트 가능
            WssClient c(io, tls, "ws.okx.com", "8443", "/ws/v5/public");
            c.open();

            ordered_json sub;
            sub["op"] = "subscribe"; sub["args"] = ordered_json::array();
            for (auto& kv : symmap) sub["args"].push_back({ {"channel","books5"},{"instId",kv.second} });
            c.send(sub.dump());

            for (;;) {
                const std::string s = c.read_text();
                const double recv = now_sec();

                auto j = json::parse(s, nullptr, false);
                if (j.is_discarded() || !j.contains("arg") || !j.contains("data")) continue;
                if (j["arg"].value("channel", "") != "books5") continue;
                if (!j["data"].is_array() || j["data"].empty()) continue;

                const auto d = j["data"][0];

                // instId -> 우리 심볼 역매핑
                const std::string venue = j["arg"].value("instId", "");
                std::string unified;
                for (auto& kv : symmap) if (kv.second == venue) { unified = kv.first; break; }
                if (unified.empty()) continue;

                OrderBookEvent ev;
                ev.exchange = "okx";
                ev.instance_id = inst;
                ev.symbol = unified;

                // --- event_ts: ts(ms) → sec ---
                double ms = d.contains("ts") ? parse_ms(d["ts"]) : 0.0;
                ev.event_ts = (ms > 0.0) ? (ms / 1000.0) : recv;
                ev.recv_ts = recv;

                // --- seq: 원본 'seq'가 있으면 사용, 없으면 ts(ms) 기반 단조 seq 합성 ---
                if (d.contains("seq")) {
                    ev.seq = d["seq"].is_string()
                        ? std::stoull(d["seq"].get<std::string>())
                        : d["seq"].get<std::uint64_t>();
                }
                else {
                    const std::string key = "okx:" + ev.symbol + ":" + ev.instance_id;
                    const uint64_t ts_ms_u64 = (uint64_t)(ms > 0.0 ? ms : recv * 1000.0);
                    ev.seq = compose_monotonic_seq(key, ts_ms_u64);
                    if (ev.seq == 0) continue; // 과거 프레임은 안전하게 드랍
                }

                // --- depth ---
                if (d.contains("bids"))
                    for (const auto& x : d["bids"]) if (x.size() >= 2)
                        ev.bids.emplace_back(std::stod(x[0].get<std::string>()),
                            std::stod(x[1].get<std::string>()));
                if (d.contains("asks"))
                    for (const auto& x : d["asks"]) if (x.size() >= 2)
                        ev.asks.emplace_back(std::stod(x[0].get<std::string>()),
                            std::stod(x[1].get<std::string>()));

                out.push(std::move(ev));
            }
        }
        catch (const std::exception& e) {
            std::cerr << "[okx] " << e.what() << " → reconnect in 3s\n";
            std::this_thread::sleep_for(std::chrono::seconds(3));
        }
    }
}

static void run_bybit(asio::io_context& io, ssl::context& tls, const std::string& inst,
    const std::map<std::string, std::string>& symmap, EventQueue& out) {
    // 구독 메시지: orderbook.50.<SYMBOL> (Spot v5)
    ordered_json sub; sub["op"] = "subscribe"; sub["args"] = ordered_json::array();
    for (auto& kv : symmap) sub["args"].push_back("orderbook.50." + kv.second);

    // 값 -> milliseconds(double) 파서 (문자열/정수/실수 안전 처리)
    auto parse_ms = [](const json& x) -> double {
        try {
            if (x.is_string())        return std::stod(x.get<std::string>());
            if (x.is_number_integer()) return static_cast<double>(x.get<long long>());
            if (x.is_number_float())   return x.get<double>();
        }
        catch (...) {}
        return 0.0;
        };

    while (running.load()) {
        try {
            WssClient c(io, tls, "stream.bybit.com", "443", "/v5/public/spot");
            c.open();
            c.send(sub.dump());

            for (;;) {
                const std::string s = c.read_text();
                const double recv = now_sec();

                auto j = json::parse(s, nullptr, false);
                if (j.is_discarded() || !j.contains("topic")) continue;

                const std::string topic = j["topic"].get<std::string>();
                if (topic.rfind("orderbook.", 0) != 0) continue;

                if (!j.contains("data")) continue;
                const auto& d = j["data"];

                // topic 끝부분의 심볼 (예: orderbook.50.BTCUSDT → BTCUSDT)
                const std::string venue = topic.substr(topic.find_last_of('.') + 1);

                std::string unified;
                for (const auto& kv : symmap) {
                    if (kv.second == venue) { unified = kv.first; break; }
                }
                if (unified.empty()) {
                    // std::cerr << "[bybit] unmatched symbol in topic: " << topic << "\n";
                    continue;
                }

                OrderBookEvent ev;
                ev.exchange = "bybit";
                ev.instance_id = inst;
                ev.symbol = unified;

                // --- event_ts 설정: ts/t 모두 대응, 단위(ms/µs/초) 판별 ---
                double ms = 0.0;
                if (d.contains("ts"))        ms = parse_ms(d["ts"]);
                else if (d.contains("t"))     ms = parse_ms(d["t"]);
                else if (j.contains("ts"))    ms = parse_ms(j["ts"]); // 드물게 최상위에 오는 경우 대비

                double secs = 0.0;
                if (ms > 1e14) secs = ms / 1e6;   // 마이크로초
                else if (ms > 1e10) secs = ms / 1e3;   // 밀리초
                else if (ms > 0.0)  secs = ms;         // 이미 초 단위라고 가정
                else                secs = recv;       // 폴백: 수신 시각

                ev.event_ts = secs;
                ev.recv_ts = recv;

                // --- seq ---
                if (d.contains("u")) {
                    ev.seq = d["u"].is_string() ? std::stoull(d["u"].get<std::string>())
                        : d["u"].get<std::uint64_t>();
                }

                // --- depth ---
                if (d.contains("b")) {
                    for (const auto& x : d["b"]) if (x.size() >= 2) {
                        ev.bids.emplace_back(std::stod(x[0].get<std::string>()),
                            std::stod(x[1].get<std::string>()));
                    }
                }
                if (d.contains("a")) {
                    for (const auto& x : d["a"]) if (x.size() >= 2) {
                        ev.asks.emplace_back(std::stod(x[0].get<std::string>()),
                            std::stod(x[1].get<std::string>()));
                    }
                }

                out.push(std::move(ev));
            }
        }
        catch (const std::exception& e) {
            std::cerr << "[bybit] " << e.what() << " → reconnect in 3s\n";
            std::this_thread::sleep_for(std::chrono::seconds(3));
        }
    }
}

// ========= 메인 (A/B × 3거래소) =========
int main() {
    std::signal(SIGINT, on_sig);
#if defined(_WIN32)
    std::signal(SIGBREAK, on_sig);
#endif

    asio::io_context io;
    auto tls = make_tls_ctx();

    EventQueue central_q;
    std::unordered_map<std::string, std::unique_ptr<LocalOrderBook>> books;

    // 저장 스레드
    std::thread saver([&] { store_worker(central_q, books); });

    // 심볼 맵
    SymbolMaps maps;

    // A/B × (Binance/OKX/Bybit)
    std::vector<std::thread> ths;
    ths.emplace_back([&] { run_binance(io, tls, "A", maps.binance, central_q); });
    ths.emplace_back([&] { run_okx(io, tls, "A", maps.okx, central_q); });
    ths.emplace_back([&] { run_bybit(io, tls, "A", maps.bybit, central_q); });
    ths.emplace_back([&] { run_binance(io, tls, "B", maps.binance, central_q); });
    ths.emplace_back([&] { run_okx(io, tls, "B", maps.okx, central_q); });
    ths.emplace_back([&] { run_bybit(io, tls, "B", maps.bybit, central_q); });

    // 메인 루프(종료 신호까지 유지)
    while (running.load())
        std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // 종료 정리
    central_q.stop();
    if (saver.joinable()) saver.join();
    for (auto& t : ths) if (t.joinable()) t.join();
    return 0;
}
