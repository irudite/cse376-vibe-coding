// astar.cpp
// Parallel shortest-path using bidirectional Dijkstra.
//
// Modes:
//   ./astar <file>                           → random benchmark (2/4/8 threads, results.csv)
//   ./astar <file> <queries> [threads]       → test-case table with per-query timing
//   ./astar <file> <queries> <threads> <s> <e> → specific pair in test-case table
//   ./astar <file> --bench <start> <end>     → load+search table across 1/2/4/8/16 threads
//
// Load-phase optimizations:
//   - fread entire file once into memory
//   - N threads parse their chunk of the buffer in parallel into local edge lists
//   - Sequential CSR construction from merged edge lists
//
// Search-phase optimizations:
//   - CSR for forward + reverse graphs
//   - Bidirectional Dijkstra (far fewer nodes settled)
//   - Custom 4-ary min-heap
//   - Thread-local g-score vectors with O(settled) touched-list reset

#include <iostream>
#include <fstream>
#include <iomanip>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <random>
#include <limits>
#include <cstdio>
#include <cstring>
#include <algorithm>
// Platform headers for cache-bypass I/O
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

using namespace std;
using Clock  = chrono::high_resolution_clock;
using ms     = chrono::duration<double, milli>;
static constexpr float INF = numeric_limits<float>::infinity();

// ─── Graph (CSR) ──────────────────────────────────────────────────────────────
struct Edge { int to; float w; };

static int          NUM_NODES      = 0;
static int          DECLARED_EDGES = 0;
static string       GRAPH_NAME;
static vector<int>  fwd_ptr, bwd_ptr;
static vector<Edge> fwd_edge, bwd_edge;

// Fast manual text parsers (much faster than sscanf for millions of lines).
inline const char* parse_int(const char* p, int& out) {
    while (*p == ' ' || *p == '\t') ++p;
    unsigned v = 0;
    while ((unsigned)(*p - '0') < 10u) v = v * 10 + (*p++ - '0');
    out = (int)v;
    return p;
}
inline const char* parse_float(const char* p, float& out) {
    while (*p == ' ' || *p == '\t') ++p;
    float v = 0, div = 1.0f;
    while ((unsigned)(*p - '0') < 10u) v = v * 10.0f + (*p++ - '0');
    if (*p == '.') { ++p; while ((unsigned)(*p - '0') < 10u) { div *= 10.0f; v += (*p++ - '0') / div; } }
    out = v;
    return p;
}

// Temporary edge struct used during parallel loading.
struct RawEdge { int u, v; float w; };

// Load graph with n_threads parallel parsers.
// Steps:
//   1. fread file into memory (sequential — disk I/O)
//   2. Split buffer at line boundaries; each thread parses its chunk
//   3. Sequential: count degrees, build CSR, fill edges
void load_graph(const string& filename, int n_threads = 1) {
    // ── 1. Read (cache-bypassed for honest timing) ────────────────────────────
    // macOS: F_NOCACHE disables the unified buffer cache on this fd.
    // Linux: posix_fadvise DONTNEED evicts cached pages before reading.
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd < 0) { cerr << "ERROR: Cannot open '" << filename << "'\n"; exit(1); }

    struct stat st{};
    fstat(fd, &st);
    size_t fsize = (size_t)st.st_size;

#ifdef __APPLE__
    fcntl(fd, F_NOCACHE, 1);          // bypass page cache on macOS
#elif defined(__linux__)
    posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);  // evict cached pages on Linux
#endif

    vector<char> buf(fsize + 1);
    size_t nread = 0;
    while (nread < fsize) {
        ssize_t r = read(fd, buf.data() + nread, fsize - nread);
        if (r <= 0) break;
        nread += r;
    }
    close(fd);
    buf[fsize] = '\0';

    const char* p   = buf.data();
    const char* end = p + fsize;

    // ── 2. Parse header comment lines ────────────────────────────────────────
    int declared_nodes = 0, hline = 0;
    while (p < end && *p == '#') {
        const char* nl = (const char*)memchr(p, '\n', end - p);
        if (!nl) nl = end;
        ++hline;
        if (hline == 2) {
            const char* s = p + 1; while (*s == ' ') ++s;
            GRAPH_NAME = string(s, nl);
        }
        if (hline == 3) sscanf(p, "# Nodes: %d Edges: %d", &declared_nodes, &DECLARED_EDGES);
        p = nl + 1;
    }
    NUM_NODES = declared_nodes;
    const char* data_start = p;
    size_t      data_len   = (size_t)(end - data_start);

    // ── 3. Split buffer into n_threads chunks at line boundaries ─────────────
    vector<const char*> chunk_start(n_threads + 1);
    chunk_start[0]         = data_start;
    chunk_start[n_threads] = end;
    for (int t = 1; t < n_threads; ++t) {
        const char* mid = data_start + (size_t)t * data_len / n_threads;
        while (mid < end && *mid != '\n') ++mid;
        if (mid < end) ++mid;
        chunk_start[t] = mid;
    }

    // ── 4. Parallel parse into thread-local edge lists ────────────────────────
    const size_t est = (size_t)(DECLARED_EDGES + n_threads - 1) / n_threads;
    vector<vector<RawEdge>> thread_edges(n_threads);
    {
        vector<thread> workers;
        workers.reserve(n_threads);
        for (int t = 0; t < n_threads; ++t) {
            workers.emplace_back([&, t]() {
                auto& local = thread_edges[t];
                local.reserve(est);
                const char* q    = chunk_start[t];
                const char* qend = chunk_start[t + 1];
                while (q < qend) {
                    int u, v; float w;
                    q = parse_int(q, u);
                    q = parse_int(q, v);
                    q = parse_float(q, w);
                    while (q < qend && *q != '\n') ++q;
                    if (q < qend) ++q;
                    if ((unsigned)u < (unsigned)NUM_NODES &&
                        (unsigned)v < (unsigned)NUM_NODES)
                        local.push_back({u, v, w});
                }
            });
        }
        for (auto& w : workers) w.join();
    }

    // ── 5. Count degrees (sequential over merged thread lists) ────────────────
    vector<int> fwd_deg(NUM_NODES, 0), bwd_deg(NUM_NODES, 0);
    for (auto& te : thread_edges)
        for (auto& e : te) { fwd_deg[e.u]++; bwd_deg[e.v]++; }

    // ── 6. Build CSR offset arrays ────────────────────────────────────────────
    fwd_ptr.assign(NUM_NODES + 1, 0);
    bwd_ptr.assign(NUM_NODES + 1, 0);
    for (int i = 0; i < NUM_NODES; ++i) {
        fwd_ptr[i+1] = fwd_ptr[i] + fwd_deg[i];
        bwd_ptr[i+1] = bwd_ptr[i] + bwd_deg[i];
    }
    fwd_edge.resize(fwd_ptr[NUM_NODES]);
    bwd_edge.resize(bwd_ptr[NUM_NODES]);

    // ── 7. Fill edges using degree arrays as write cursors ────────────────────
    fill(fwd_deg.begin(), fwd_deg.end(), 0);
    fill(bwd_deg.begin(), bwd_deg.end(), 0);
    for (auto& te : thread_edges)
        for (auto& e : te) {
            fwd_edge[fwd_ptr[e.u] + fwd_deg[e.u]++] = {e.v, e.w};
            bwd_edge[bwd_ptr[e.v] + bwd_deg[e.v]++] = {e.u, e.w};
        }
}

void clear_graph() {
    vector<int>().swap(fwd_ptr);   vector<int>().swap(bwd_ptr);
    vector<Edge>().swap(fwd_edge); vector<Edge>().swap(bwd_edge);
}

// ─── 4-ary Min-Heap ───────────────────────────────────────────────────────────
struct MinHeap {
    struct Entry { float key; int val; };
    vector<Entry> data;
    static constexpr int D = 4;

    void  clear()           { data.clear(); }
    bool  empty()     const { return data.empty(); }
    float top_key()   const { return data[0].key; }
    void  reserve(size_t n) { data.reserve(n); }

    void push(float key, int val) {
        int i = (int)data.size();
        data.push_back({key, val});
        while (i > 0) {
            int par = (i - 1) / D;
            if (data[par].key <= data[i].key) break;
            swap(data[par], data[i]); i = par;
        }
    }
    pair<float,int> pop() {
        auto ret = make_pair(data[0].key, data[0].val);
        int n = (int)data.size() - 1;
        data[0] = data[n]; data.pop_back();
        int i = 0;
        while (true) {
            int best = i, base = D*i+1, lim = min(base+D, n);
            for (int c = base; c < lim; ++c)
                if (data[c].key < data[best].key) best = c;
            if (best == i) break;
            swap(data[i], data[best]); i = best;
        }
        return ret;
    }
};

// ─── Per-thread Search State ──────────────────────────────────────────────────
struct SearchState {
    vector<float> gf, gb;
    vector<int>   tf, tb;
    MinHeap       pqf, pqb;
    explicit SearchState(int n) : gf(n, INF), gb(n, INF) {
        tf.reserve(1<<17); tb.reserve(1<<17);
        pqf.reserve(1<<17); pqb.reserve(1<<17);
    }
};

// ─── Bidirectional Dijkstra ───────────────────────────────────────────────────
float bidir_dijkstra(int src, int dst, SearchState& s) {
    if (src == dst) return 0.0f;
    s.tf.clear(); s.tb.clear(); s.pqf.clear(); s.pqb.clear();

    auto set_gf = [&](int v, float ng) { if (s.gf[v]==INF) s.tf.push_back(v); s.gf[v]=ng; };
    auto set_gb = [&](int v, float ng) { if (s.gb[v]==INF) s.tb.push_back(v); s.gb[v]=ng; };

    set_gf(src, 0.0f); s.pqf.push(0.0f, src);
    set_gb(dst, 0.0f); s.pqb.push(0.0f, dst);
    float mu = INF;

    while (!s.pqf.empty() && !s.pqb.empty()) {
        if (s.pqf.top_key() + s.pqb.top_key() >= mu) break;
        if (s.pqf.top_key() <= s.pqb.top_key()) {
            auto [d, u] = s.pqf.pop();
            if (d > s.gf[u]) continue;
            for (int i = fwd_ptr[u], ie = fwd_ptr[u+1]; i < ie; ++i) {
                const Edge& e = fwd_edge[i];
                float ng = s.gf[u] + e.w;
                if (ng < s.gf[e.to]) {
                    set_gf(e.to, ng); s.pqf.push(ng, e.to);
                    if (s.gb[e.to] < INF) mu = min(mu, ng + s.gb[e.to]);
                }
            }
        } else {
            auto [d, u] = s.pqb.pop();
            if (d > s.gb[u]) continue;
            for (int i = bwd_ptr[u], ie = bwd_ptr[u+1]; i < ie; ++i) {
                const Edge& e = bwd_edge[i];
                float ng = s.gb[u] + e.w;
                if (ng < s.gb[e.to]) {
                    set_gb(e.to, ng); s.pqb.push(ng, e.to);
                    if (s.gf[e.to] < INF) mu = min(mu, s.gf[e.to] + ng);
                }
            }
        }
    }
    for (int n : s.tf) s.gf[n] = INF;
    for (int n : s.tb) s.gb[n] = INF;
    return mu < INF ? mu : -1.0f;
}

// ─── Helpers ──────────────────────────────────────────────────────────────────
string fmt(long long n) {
    string s = to_string(n);
    for (int i = (int)s.size() - 3; i > 0; i -= 3) s.insert(i, ",");
    return s;
}

vector<pair<int,int>> gen_queries(int n, int seed = 42) {
    mt19937 rng(seed);
    uniform_int_distribution<int> nd(0, NUM_NODES - 1);
    vector<pair<int,int>> qs; qs.reserve(n);
    while ((int)qs.size() < n) { int s=nd(rng), d=nd(rng); if (s!=d) qs.push_back({s,d}); }
    return qs;
}

// ─── Benchmark Mode (--bench) ─────────────────────────────────────────────────
// For each thread count: reload graph, time load; run query N times, time search.
void run_pair_bench(const string& filename, int src, int dst) {
    constexpr int THREAD_COUNTS[] = {1, 2, 4, 8, 16};
    constexpr int SEARCH_REPS     = 50; // repetitions for stable search timing

    cout << left
         << setw(10) << "Threads"
         << setw(15) << "Load (ms)"
         << setw(15) << "Search (ms)"
         << "Total Time (ms)\n";
    cout << string(60, '-') << "\n";

    for (int nt : THREAD_COUNTS) {
        // ── Load (timed) ─────────────────────────────────────────────────────
        clear_graph();
        auto t0 = Clock::now();
        load_graph(filename, nt);
        double load_ms = ms(Clock::now() - t0).count();

        if ((unsigned)src >= (unsigned)NUM_NODES ||
            (unsigned)dst >= (unsigned)NUM_NODES) {
            cerr << "ERROR: Node ID out of range [0, " << NUM_NODES-1 << "]\n";
            exit(1);
        }

        // ── Search (timed): warm-up then average over SEARCH_REPS ─────────────
        SearchState state(NUM_NODES);
        bidir_dijkstra(src, dst, state);   // warm up caches

        auto t1 = Clock::now();
        float dist = -1.0f;
        for (int r = 0; r < SEARCH_REPS; ++r)
            dist = bidir_dijkstra(src, dst, state);
        double search_ms = ms(Clock::now() - t1).count() / SEARCH_REPS;

        double total_ms = load_ms + search_ms;

        cout << fixed << setprecision(2)
             << left  << setw(10) << nt
             << left  << setw(15) << load_ms
             << left  << setw(15) << search_ms
             << total_ms << "\n";

        // Write CSV row for plotting (append mode — reopen per thread count for simplicity)
        if (nt == THREAD_COUNTS[0]) {
            ofstream csv("pair_bench.csv");
            csv << "threads,load_ms,search_ms,total_ms,distance\n";
        }
        ofstream csv("pair_bench.csv", ios::app);
        csv << fixed << setprecision(4)
            << nt << "," << load_ms << "," << search_ms << ","
            << total_ms << "," << (dist < 0 ? 0.f : dist) << "\n";
    }

    cout << "\nFrom node " << src << " to " << dst << "\n";
}

// ─── Test-Case Mode ───────────────────────────────────────────────────────────
struct QResult { double time_ms; float distance; };

void run_test_mode(const vector<pair<int,int>>& queries, int n_threads) {
    const int Q = (int)queries.size();
    cout << "Randomly sampling " << Q << " start/end node pairs\n"
         << GRAPH_NAME << ": " << fmt(NUM_NODES) << " nodes, "
         << fmt(DECLARED_EDGES) << " edges\n"
         << "Using " << n_threads << " thread(s)\n"
         << string(48, '=') << "\n\n"
         << left << setw(5) << "#" << setw(11) << "Start"
         << setw(11) << "End" << setw(14) << "Time(ms)" << "Distance\n"
         << string(48, '-') << "\n";
    cout.flush();

    vector<QResult> results(Q);
    atomic<int> next_idx{0};
    auto worker = [&] {
        SearchState state(NUM_NODES);
        int i;
        while ((i = next_idx.fetch_add(1, memory_order_relaxed)) < Q) {
            auto t0 = Clock::now();
            float d = bidir_dijkstra(queries[i].first, queries[i].second, state);
            results[i] = {ms(Clock::now() - t0).count(), d};
        }
    };
    vector<thread> workers; workers.reserve(n_threads);
    for (int i = 0; i < n_threads; ++i) workers.emplace_back(worker);
    for (auto& t : workers) t.join();

    for (int i = 0; i < Q; ++i) {
        const auto& r = results[i];
        cout << left  << setw(5) << (i+1)
             << left  << setw(11) << queries[i].first
             << left  << setw(11) << queries[i].second
             << fixed << setprecision(3) << setw(14) << r.time_ms;
        if (r.distance < 0.0f) cout << "FAILED"; else cout << fixed << setprecision(3) << r.distance;
        cout << "\n";
    }
    cout << "\nSampling complete!\n\n"
         << "Tips for choosing test cases:\n"
         << "  - Short distances (< 100):       Good for correctness testing\n"
         << "  - Medium distances (100-1000):   Balanced workload\n"
         << "  - Long distances (> 1000):       Stress test for performance\n"
         << "  - Cases with FAILED or -1:       Disconnected components (no path)\n";
}

// ─── Random Benchmark Mode (default) ─────────────────────────────────────────
struct RunResult { double latency_s, total_distance; int paths_found; };

RunResult benchmark_run(const vector<pair<int,int>>& queries, int n_threads) {
    const int Q = (int)queries.size();
    vector<float> dist(Q, -1.0f);
    atomic<int> next_idx{0};
    auto worker = [&] {
        SearchState state(NUM_NODES);
        for (int i; (i = next_idx.fetch_add(1, memory_order_relaxed)) < Q;)
            dist[i] = bidir_dijkstra(queries[i].first, queries[i].second, state);
    };
    auto t0 = Clock::now();
    vector<thread> workers; workers.reserve(n_threads);
    for (int i = 0; i < n_threads; ++i) workers.emplace_back(worker);
    for (auto& t : workers) t.join();
    double latency = chrono::duration<double>(Clock::now() - t0).count();
    double total = 0.0; int found = 0;
    for (float d : dist) if (d >= 0.0f) { total += d; ++found; }
    return {latency, total, found};
}

void run_benchmark_mode(const vector<pair<int,int>>& queries) {
    constexpr int NUM_RUNS        = 5;
    constexpr int THREAD_COUNTS[] = {2, 4, 8};
    ofstream csv("results.csv");
    csv << fixed << setprecision(6) << "threads,run,latency_s,total_distance,paths_found\n";
    for (int nt : THREAD_COUNTS) {
        cerr << "[" << nt << " thread(s)]\n";
        for (int r = 1; r <= NUM_RUNS; ++r) {
            RunResult res = benchmark_run(queries, nt);
            cerr << "  run " << r << "  latency=" << fixed << setprecision(4)
                 << res.latency_s << "s  total_dist=" << setprecision(2)
                 << res.total_distance << "  found=" << res.paths_found
                 << "/" << (int)queries.size() << "\n";
            csv << nt << "," << r << "," << setprecision(6) << res.latency_s << ","
                << setprecision(4) << res.total_distance << "," << res.paths_found << "\n";
        }
        cerr << "\n";
    }
    cerr << "Saved results.csv\n";
}

// ─── Main ─────────────────────────────────────────────────────────────────────
int main(int argc, char* argv[]) {
    const string filename = (argc > 1) ? argv[1] : "/home/palmieri/aaa.txt";

    if (argc == 5 && string(argv[2]) == "--bench") {
        // Load+search table: ./astar <file> --bench <start> <end>
        int src = atoi(argv[3]), dst = atoi(argv[4]);
        run_pair_bench(filename, src, dst);

    } else if (argc == 6) {
        // Specific pair test: ./astar <file> <queries> <threads> <start> <end>
        int nt = max(1, atoi(argv[3]));
        int src = atoi(argv[4]), dst = atoi(argv[5]);
        cerr << "Loading graph...\n";
        load_graph(filename);
        cerr << "Done.\n";
        if ((unsigned)src >= (unsigned)NUM_NODES || (unsigned)dst >= (unsigned)NUM_NODES) {
            cerr << "ERROR: Node ID out of range [0, " << NUM_NODES-1 << "]\n"; return 1;
        }
        run_test_mode({{src, dst}}, nt);

    } else if (argc >= 3) {
        // Random test-case table: ./astar <file> <queries> [threads]
        int nq = max(1, atoi(argv[2]));
        int nt = (argc >= 4) ? max(1, atoi(argv[3])) : 1;
        cerr << "Loading graph...\n";
        load_graph(filename);
        cerr << "Done.\n";
        run_test_mode(gen_queries(nq), nt);

    } else {
        // Default random benchmark: ./astar <file>
        cerr << "Loading graph from '" << filename << "'...\n";
        load_graph(filename);
        cerr << "Graph loaded: " << NUM_NODES << " nodes, " << fwd_ptr[NUM_NODES] << " edges.\n";
        cerr << "Running benchmark (500 queries)...\n\n";
        run_benchmark_mode(gen_queries(500));
    }
    return 0;
}
