#!/usr/bin/env python3
"""
run_experiments.py — Experiment orchestration for the distributed KV store.

Reads experiment configurations from config/experiment_configs.json.
For each configuration × trial:
  1. Starts the secondary node (Node 1)
  2. Starts the primary node (Node 0, with heartbeat + replication)
  3. Starts the workload generator targeting Node 0
  4. Waits for warmup
  5. Injects the configured fault (crash or delay)
  6. Waits for detection + post-fault observation window
  7. Stops all processes and collects logs
  8. Records injector metadata (t_fail, t_detect, etc.)

After all trials: runs compute_metrics.py and plot_results.py.
"""

import json
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BIN_DIR = ROOT / "build"
KVNODE = BIN_DIR / "kvnode"
WORKLOAD = BIN_DIR / "kv_workload"
CONFIG_PATH = ROOT / "config" / "experiment_configs.json"
OUTPUT_DIR = ROOT / "output"


def wall_ms() -> int:
    return int(time.time() * 1000)


def wait_for_port(port: int, timeout: float = 5.0) -> bool:
    """Wait until a TCP port is accepting connections."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(0.1)
            s.connect(("127.0.0.1", port))
            s.close()
            return True
        except (ConnectionRefusedError, OSError):
            time.sleep(0.05)
    return False


def send_fault_delay(port: int, delay_ms: int) -> bool:
    """Send a FAULT_DELAY message to a node to inject application-level delay."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(2.0)
        s.connect(("127.0.0.1", port))
        msg = json.dumps({"type": "FAULT_DELAY", "delay_ms": delay_ms}) + "\n"
        s.sendall(msg.encode())
        resp = s.recv(1024)
        s.close()
        return b"FAULT_DELAY_ACK" in resp
    except Exception as e:
        print(f"  Warning: failed to send FAULT_DELAY: {e}", file=sys.stderr)
        return False


def wait_for_declared_dead(log_path: Path, run_id: str, t_fail: int,
                           timeout_sec: float = 15.0):
    """Poll the primary's log for a declared_dead event after t_fail."""
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        if log_path.exists():
            try:
                with open(log_path) as f:
                    for line in f:
                        try:
                            obj = json.loads(line)
                            if (obj.get("event") == "declared_dead" and
                                    obj.get("run_id") == run_id):
                                ts = obj.get("ts_ms")
                                if ts is not None and ts >= t_fail - 200:
                                    return ts
                        except json.JSONDecodeError:
                            continue
            except OSError:
                pass
        time.sleep(0.05)
    return None


def kill_process(proc: subprocess.Popen, use_sigkill: bool = True):
    """Kill a process reliably."""
    if proc.poll() is not None:
        return
    try:
        if use_sigkill:
            proc.send_signal(signal.SIGKILL)
        else:
            proc.terminate()
        proc.wait(timeout=3)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
    except Exception:
        pass


def run_one_trial(config: dict, base: dict, trial_num: int, trial_dir: Path):
    """Run a single experiment trial."""
    name = config["name"]
    hb_interval = config["hb_interval_ms"]
    hb_timeout = config["hb_timeout_ms"]
    repl_mode = config["repl_mode"]
    fault_type = config["fault_type"]
    fd_algo = config.get("fd_algo", "fixed")
    phi_threshold = config.get("phi_threshold", 8.0)

    port0 = base.get("node_ports", [9100, 9101])[0]
    port1 = base.get("node_ports", [9100, 9101])[1]
    warmup_sec = base.get("warmup_sec", 3.0)
    fault_time_sec = base.get("fault_time_sec", 5.0)
    experiment_duration = base.get("experiment_duration_sec", 10.0)
    wl_num_ops    = base.get("workload_num_ops", 500)
    wl_set_ratio  = base.get("workload_set_ratio", 0.5)
    wl_rate       = base.get("workload_rate", 100)
    # Per-config overrides take priority over base defaults
    wl_num_clients = config.get("workload_num_clients",
                                 base.get("workload_num_clients", 1))
    wl_zipf_alpha  = config.get("workload_zipf_alpha",
                                 base.get("workload_zipf_alpha", 0.0))

    run_id = f"{name}_t{trial_num}_{wall_ms()}"
    trial_dir.mkdir(parents=True, exist_ok=True)

    log_n0 = trial_dir / "node0.jsonl"
    log_n1 = trial_dir / "node1.jsonl"
    log_wl = trial_dir / "workload.jsonl"
    injector_log = trial_dir / "injector.jsonl"
    wal_n0 = trial_dir / "node0.wal"
    wal_n1 = trial_dir / "node1.wal"

    env = os.environ.copy()
    env["RUN_ID"] = run_id

    procs = []

    try:
        # 1. Start secondary (Node 1)
        n1_cmd = [
            str(KVNODE),
            "--id", "node1", "--port", str(port1),
            "--log_path", str(log_n1),
            "--run_id", run_id,
            "--hb_interval_ms", str(hb_interval),
            "--hb_timeout_ms", str(hb_timeout),
            "--wal_path", str(wal_n1),
        ]
        proc_n1 = subprocess.Popen(
            n1_cmd,
            stdout=(trial_dir / "node1.out").open("w"),
            stderr=subprocess.STDOUT,
            env=env,
        )
        procs.append(proc_n1)
        if not wait_for_port(port1, timeout=5.0):
            print(f"  Warning: Node 1 port {port1} not ready", file=sys.stderr)

        # 2. Start primary (Node 0)
        n0_cmd = [
            str(KVNODE),
            "--id", "node0", "--port", str(port0), "--primary",
            "--peer", f"127.0.0.1:{port1}",
            "--log_path", str(log_n0),
            "--run_id", run_id,
            "--hb_interval_ms", str(hb_interval),
            "--hb_timeout_ms", str(hb_timeout),
            "--repl_mode", repl_mode,
            "--wal_path", str(wal_n0),
            "--fd_algo", fd_algo,
            "--phi_threshold", str(phi_threshold),
        ]
        proc_n0 = subprocess.Popen(
            n0_cmd,
            stdout=(trial_dir / "node0.out").open("w"),
            stderr=subprocess.STDOUT,
            env=env,
        )
        procs.append(proc_n0)
        if not wait_for_port(port0, timeout=5.0):
            print(f"  Warning: Node 0 port {port0} not ready", file=sys.stderr)

        # 3. Start workload generator (targets Node 0)
        wl_cmd = [
            str(WORKLOAD),
            "--target", f"127.0.0.1:{port0}",
            "--num_ops", str(wl_num_ops),
            "--set_ratio", str(wl_set_ratio),
            "--rate", str(wl_rate),
            "--num_clients", str(wl_num_clients),
            "--zipf_alpha", str(wl_zipf_alpha),
            "--log_path", str(log_wl),
            "--run_id", run_id,
        ]
        proc_wl = subprocess.Popen(
            wl_cmd,
            stdout=(trial_dir / "workload.out").open("w"),
            stderr=subprocess.STDOUT,
            env=env,
        )
        procs.append(proc_wl)

        # 4. Warmup
        time.sleep(warmup_sec)

        # 5. Inject fault
        t_fault = wall_ms()
        injector_events = [{
            "event": "run_start",
            "run_id": run_id,
            "ts_ms": t_fault - int(warmup_sec * 1000),
            "config": name,
            "hb_interval_ms": hb_interval,
            "hb_timeout_ms": hb_timeout,
            "repl_mode": repl_mode,
            "fd_algo": fd_algo,
            "phi_threshold": phi_threshold,
            "fault_type": fault_type,
            "workload_num_clients": wl_num_clients,
            "workload_zipf_alpha": wl_zipf_alpha,
        }]

        if fault_type == "crash":
            # Kill Node 1 (secondary)
            injector_events.append({
                "event": "fault_inject",
                "run_id": run_id,
                "ts_ms": t_fault,
                "fault_type": "crash",
                "target": "node1",
            })
            kill_process(proc_n1, use_sigkill=True)
            procs.remove(proc_n1)

        elif fault_type == "delay":
            # Inject application-level delay on Node 1
            delay_ms = config.get("delay_ms", 500)
            ok = send_fault_delay(port1, delay_ms)
            injector_events.append({
                "event": "fault_inject",
                "run_id": run_id,
                "ts_ms": t_fault,
                "fault_type": "delay",
                "target": "node1",
                "delay_ms": delay_ms,
                "success": ok,
            })

        # 6. Wait for detection and post-fault observation
        post_fault_sec = experiment_duration - fault_time_sec
        t_detect = wait_for_declared_dead(
            log_n0, run_id, t_fault,
            timeout_sec=max(10.0, hb_timeout / 1000.0 * 5 + 5.0))

        detection_latency_ms = (t_detect - t_fault) if t_detect is not None else None

        injector_events.append({
            "event": "detection_result",
            "run_id": run_id,
            "t_fault_ms": t_fault,
            "t_detect_ms": t_detect,
            "detection_latency_ms": detection_latency_ms,
        })

        # Wait remaining time for workload to continue through failure
        if post_fault_sec > 0:
            time.sleep(max(0, post_fault_sec))

    finally:
        # 7. Stop all processes
        for proc in procs:
            kill_process(proc, use_sigkill=False)

        # 8. Write injector log
        with open(injector_log, "w") as f:
            for evt in injector_events:
                f.write(json.dumps(evt) + "\n")

    result = {
        "run_id": run_id,
        "config": name,
        "trial": trial_num,
        "hb_interval_ms": hb_interval,
        "hb_timeout_ms": hb_timeout,
        "repl_mode": repl_mode,
        "fault_type": fault_type,
        "fd_algo": fd_algo,
        "phi_threshold": phi_threshold,
        "workload_num_clients": wl_num_clients,
        "workload_zipf_alpha": wl_zipf_alpha,
        "t_fault_ms": t_fault,
        "t_detect_ms": t_detect,
        "detection_latency_ms": detection_latency_ms,
    }
    print(f"  Trial {trial_num}: detection_latency_ms = {detection_latency_ms}")
    return result



def main():
    import argparse
    parser = argparse.ArgumentParser(description="Run distributed KV store experiments")
    parser.add_argument("--config", type=Path, default=CONFIG_PATH,
                        help="Path to experiment config JSON")
    parser.add_argument("--output", type=Path, default=OUTPUT_DIR,
                        help="Output directory")
    parser.add_argument("--filter", type=str, default=None,
                        help="Only run configs matching this name substring")
    parser.add_argument("--trials", type=int, default=None,
                        help="Override number of trials per config")
    parser.add_argument("--skip-analysis", action="store_true",
                        help="Skip running compute_metrics.py and plot_results.py")
    args = parser.parse_args()

    # Check binaries exist
    if not KVNODE.exists() or not WORKLOAD.exists():
        print(f"Error: Build first: cd {ROOT} && mkdir -p build && cd build && cmake .. && make",
              file=sys.stderr)
        sys.exit(1)

    # Load config
    with open(args.config) as f:
        config_data = json.load(f)

    base = config_data["base"]
    configurations = config_data["configurations"]

    if args.filter:
        configurations = [c for c in configurations if args.filter in c["name"]]
        if not configurations:
            print(f"No configs matching filter '{args.filter}'", file=sys.stderr)
            sys.exit(1)

    trials = args.trials or base.get("trials_per_config", 5)
    logs_dir = args.output / "logs"

    all_results = []

    for cfg in configurations:
        name = cfg["name"]
        print(f"\n{'='*60}")
        print(f"Config: {name} (hb_i={cfg['hb_interval_ms']} hb_t={cfg['hb_timeout_ms']} "
              f"repl={cfg['repl_mode']} fault={cfg['fault_type']})")
        print(f"{'='*60}")

        for t in range(1, trials + 1):
            trial_dir = logs_dir / name / f"trial_{t}"
            print(f"  Starting trial {t}/{trials}...")
            try:
                result = run_one_trial(cfg, base, t, trial_dir)
                all_results.append(result)
            except Exception as e:
                print(f"  Trial {t} failed: {e}", file=sys.stderr)
                all_results.append({
                    "config": name, "trial": t, "error": str(e)
                })
            # Brief pause between trials for port cleanup
            time.sleep(1.0)

    # Save all results
    results_dir = args.output / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    results_path = results_dir / "all_trials.json"
    with open(results_path, "w") as f:
        json.dump(all_results, f, indent=2)
    print(f"\nAll trial results saved to {results_path}")

    # Run analysis
    if not args.skip_analysis:
        scripts_dir = ROOT / "scripts"
        print("\nRunning metrics computation...")
        subprocess.run([sys.executable, str(scripts_dir / "compute_metrics.py"),
                        "--output", str(args.output)], cwd=str(ROOT))
        print("Running plot generation...")
        subprocess.run([sys.executable, str(scripts_dir / "plot_results.py"),
                        "--output", str(args.output)], cwd=str(ROOT))

    print("\nDone! Results in:", args.output)


if __name__ == "__main__":
    main()
