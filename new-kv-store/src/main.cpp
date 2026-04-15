/**
 * main.cpp — Entry point for the distributed KV store node.
 *
 * New flags added for graduate-level research:
 *   --wal_path <path>       Write-Ahead Log file (JSONL). If set, every SET
 *                           is durably logged before in-memory write. On restart
 *                           with the same path, state is recovered automatically.
 *   --fd_algo <algo>        Failure detector algorithm: "fixed" (default) or "phi"
 *   --phi_threshold <float> φ threshold for Phi Accrual detector (default: 8.0)
 *   --phi_window <n>        Sample window size for Phi Accrual (default: 200)
 */

#include <csignal>
#include <cstdlib>
#include <iostream>
#include <string>

#include "node/node.hpp"

static kvs::Node* g_node = nullptr;

static void signal_handler(int sig) {
    (void)sig;
    kvs::Node::global_shutdown().store(true);
    if (g_node) g_node->stop();
}

static void usage(const char* prog) {
    std::cerr
        << "Usage: " << prog << " [options]\n"
        << "\n"
        << "Required:\n"
        << "  --id <name>             Node identifier (e.g. node0, node1)\n"
        << "  --port <port>           TCP port to listen on\n"
        << "  --log_path <path>       Path to JSONL log file\n"
        << "\n"
        << "Optional:\n"
        << "  --primary               Mark this node as the primary\n"
        << "  --peer <host:port>      Peer node address (required if --primary)\n"
        << "  --run_id <id>           Experiment run identifier\n"
        << "  --hb_interval_ms <ms>   Heartbeat interval in ms (default: 100)\n"
        << "  --hb_timeout_ms <ms>    Heartbeat timeout in ms, fixed-algo only (default: 400)\n"
        << "  --repl_mode <mode>      Replication: none|sync|async (default: none)\n"
        << "  --wal_path <path>       Write-Ahead Log file path (JSONL); disabled if absent\n"
        << "  --fd_algo <algo>        Failure detector: fixed|phi (default: fixed)\n"
        << "  --phi_threshold <val>   Phi Accrual threshold (default: 8.0)\n"
        << "  --phi_window <n>        Phi Accrual sample window (default: 200)\n"
        << "\n"
        << "Example (fixed FD, WAL enabled):\n"
        << "  " << prog << " --id node0 --port 9100 --primary --peer 127.0.0.1:9101 \\\n"
        << "       --log_path logs/n0.jsonl --wal_path logs/n0.wal --repl_mode sync\n"
        << "\n"
        << "Example (Phi Accrual FD):\n"
        << "  " << prog << " --id node0 --port 9100 --primary --peer 127.0.0.1:9101 \\\n"
        << "       --log_path logs/n0.jsonl --fd_algo phi --phi_threshold 8.0\n";
}

int main(int argc, char* argv[]) {
    signal(SIGPIPE, SIG_IGN);

    kvs::NodeConfig cfg;
    bool show_help = false;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if      (arg == "--help"  || arg == "-h")        { show_help = true; }
        else if (arg == "--id"           && i+1 < argc)  { cfg.id           = argv[++i]; }
        else if (arg == "--port"         && i+1 < argc)  { cfg.port         = std::stoi(argv[++i]); }
        else if (arg == "--peer"         && i+1 < argc)  { cfg.peer_addr    = argv[++i]; }
        else if (arg == "--log_path"     && i+1 < argc)  { cfg.log_path     = argv[++i]; }
        else if (arg == "--wal_path"     && i+1 < argc)  { cfg.wal_path     = argv[++i]; }  // NEW
        else if (arg == "--run_id"       && i+1 < argc)  { cfg.run_id       = argv[++i]; }
        else if (arg == "--hb_interval_ms" && i+1 < argc){ cfg.hb_interval_ms = std::stoi(argv[++i]); }
        else if (arg == "--hb_timeout_ms"  && i+1 < argc){ cfg.hb_timeout_ms  = std::stoi(argv[++i]); }
        else if (arg == "--repl_mode"    && i+1 < argc)  { cfg.repl_mode    = argv[++i]; }
        else if (arg == "--fd_algo"      && i+1 < argc)  { cfg.fd_algo      = argv[++i]; }  // NEW
        else if (arg == "--phi_threshold"&& i+1 < argc)  { cfg.phi_threshold= std::stod(argv[++i]); } // NEW
        else if (arg == "--phi_window"   && i+1 < argc)  { cfg.phi_window   = std::stoi(argv[++i]); } // NEW
        else if (arg == "--primary")                      { cfg.is_primary   = true; }
        else {
            std::cerr << "Unknown argument: " << arg << "\n";
            usage(argv[0]);
            return 1;
        }
    }

    if (show_help) { usage(argv[0]); return 0; }

    if (cfg.run_id == "default_run") {
        const char* env = std::getenv("RUN_ID");
        if (env && env[0] != '\0') cfg.run_id = env;
    }

    // Validation
    if (cfg.id.empty() || cfg.port <= 0 || cfg.log_path.empty()) {
        std::cerr << "Error: --id, --port, and --log_path are required\n\n";
        usage(argv[0]);
        return 1;
    }
    if (cfg.is_primary && cfg.peer_addr.empty()) {
        std::cerr << "Error: --primary requires --peer <host:port>\n\n";
        usage(argv[0]);
        return 1;
    }
    if (cfg.repl_mode != "none" && cfg.repl_mode != "sync" && cfg.repl_mode != "async") {
        std::cerr << "Error: --repl_mode must be none, sync, or async\n\n";
        usage(argv[0]);
        return 1;
    }
    if (cfg.fd_algo != "fixed" && cfg.fd_algo != "phi") {
        std::cerr << "Error: --fd_algo must be fixed or phi\n\n";
        usage(argv[0]);
        return 1;
    }
    if (cfg.hb_interval_ms <= 0 || cfg.hb_timeout_ms <= 0) {
        std::cerr << "Error: hb_interval_ms and hb_timeout_ms must be positive\n\n";
        usage(argv[0]);
        return 1;
    }
    if (cfg.phi_threshold <= 0.0) {
        std::cerr << "Error: phi_threshold must be positive\n\n";
        usage(argv[0]);
        return 1;
    }

    signal(SIGINT,  signal_handler);
    signal(SIGTERM, signal_handler);

    kvs::Node node(cfg);
    g_node = &node;
    node.run();
    g_node = nullptr;

    return 0;
}
