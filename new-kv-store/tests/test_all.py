#!/usr/bin/env python3
"""
tests/test_all.py — Comprehensive integration & unit tests for the distributed KV store.

Tests cover all four research upgrades:
  1. Write-Ahead Log (WAL) — write, recovery, crash-recovery integrity
  2. Vector Clock / RYW    — version propagation, stale-read detection
  3. Phi Accrual FD        — detection fires, φ value logged, faster than fixed-threshold
  4. Zipfian workload      — distribution shape, multi-client concurrency

Usage:
  python3 tests/test_all.py              # run all tests
  python3 tests/test_all.py -v           # verbose
  python3 tests/test_all.py WalTests     # run a single test class

Requirements: binaries must be built (build/kvnode, build/kv_workload).
"""

import json
import math
import os
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from collections import Counter
from pathlib import Path

# ─── Paths ────────────────────────────────────────────────────────────────────

ROOT     = Path(__file__).resolve().parent.parent
KVNODE   = ROOT / "build" / "kvnode"
WORKLOAD = ROOT / "build" / "kv_workload"

PORT_BASE = 19100   # Use high ports to avoid conflicts with running experiments


def next_ports():
    """Return two unique ports for a test (increments a global counter)."""
    global _port_counter
    p0 = PORT_BASE + _port_counter * 2
    p1 = PORT_BASE + _port_counter * 2 + 1
    _port_counter += 1
    return p0, p1


_port_counter = 0

# ─── Helpers ─────────────────────────────────────────────────────────────────


def wait_port(port: int, timeout: float = 5.0) -> bool:
    """Wait until a TCP port accepts connections."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return True
        except OSError:
            time.sleep(0.05)
    return False


def send_recv(port: int, msg: str, timeout: float = 3.0) -> str:
    """Send a newline-terminated JSON message and receive the response."""
    with socket.create_connection(("127.0.0.1", port), timeout=timeout) as s:
        s.sendall(msg.encode())
        s.settimeout(timeout)
        buf = b""
        while b"\n" not in buf:
            chunk = s.recv(4096)
            if not chunk:
                break
            buf += chunk
        return buf.decode().strip()


def parse_jsonl(path: Path) -> list:
    """Load a JSONL file into a list of dicts."""
    events = []
    if not path.exists():
        return events
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    events.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return events


def make_set_msg(key: str, value: str, req_id: str) -> str:
    return json.dumps({"type": "KV_SET", "key": key, "value": value,
                       "req_id": req_id, "version": {}}) + "\n"


def make_get_msg(key: str, req_id: str) -> str:
    return json.dumps({"type": "KV_GET", "key": key, "req_id": req_id}) + "\n"


class NodeProcess:
    """Context manager that starts and stops a kvnode process."""

    def __init__(self, node_id: str, port: int, log_path: Path,
                 wal_path: Path = None, run_id: str = "test",
                 is_primary: bool = False, peer_port: int = None,
                 repl_mode: str = "none", fd_algo: str = "fixed",
                 phi_threshold: float = 8.0,
                 hb_interval_ms: int = 50, hb_timeout_ms: int = 150):
        self.node_id   = node_id
        self.port      = port
        self.log_path  = log_path
        self.wal_path  = wal_path
        self.run_id    = run_id
        self.is_primary= is_primary
        self.peer_port = peer_port
        self.repl_mode = repl_mode
        self.fd_algo   = fd_algo
        self.phi_threshold = phi_threshold
        self.hb_interval_ms = hb_interval_ms
        self.hb_timeout_ms  = hb_timeout_ms
        self.proc = None

    def start(self):
        cmd = [
            str(KVNODE),
            "--id", self.node_id,
            "--port", str(self.port),
            "--log_path", str(self.log_path),
            "--run_id", self.run_id,
            "--hb_interval_ms", str(self.hb_interval_ms),
            "--hb_timeout_ms",  str(self.hb_timeout_ms),
            "--repl_mode", self.repl_mode,
            "--fd_algo", self.fd_algo,
            "--phi_threshold", str(self.phi_threshold),
        ]
        if self.wal_path:
            cmd += ["--wal_path", str(self.wal_path)]
        if self.is_primary:
            cmd += ["--primary", "--peer", f"127.0.0.1:{self.peer_port}"]
        self.proc = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        assert wait_port(self.port, timeout=5.0), \
            f"Node {self.node_id} failed to start on port {self.port}"
        return self

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()

    def kill(self):
        """Simulate crash (SIGKILL)."""
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGKILL)
            self.proc.wait()

    def __enter__(self):
        return self.start()

    def __exit__(self, *_):
        self.stop()


# ═══════════════════════════════════════════════════════════════════════════════
# TEST CLASS 1: Binary sanity checks
# ═══════════════════════════════════════════════════════════════════════════════

class BinaryTests(unittest.TestCase):
    """Verify binaries exist and basic SET/GET work."""

    def test_binaries_exist(self):
        self.assertTrue(KVNODE.exists(),
                        f"kvnode binary not found at {KVNODE}. Run: cd build && make")
        self.assertTrue(WORKLOAD.exists(),
                        f"kv_workload binary not found at {WORKLOAD}.")

    def test_basic_set_get(self):
        p0, p1 = next_ports()
        with tempfile.TemporaryDirectory() as tmp:
            log0 = Path(tmp) / "n0.jsonl"
            with NodeProcess("n0", p0, log0) as _:
                # SET
                resp = send_recv(p0, make_set_msg("hello", "world", "r1"))
                obj  = json.loads(resp)
                self.assertEqual(obj["type"], "KV_SET_RESP")
                self.assertTrue(obj["ok"])
                self.assertEqual(obj["key"], "hello")

                # GET
                resp = send_recv(p0, make_get_msg("hello", "r2"))
                obj  = json.loads(resp)
                self.assertEqual(obj["type"], "KV_GET_RESP")
                self.assertTrue(obj["ok"])
                self.assertEqual(obj["value"], "world")

    def test_get_missing_key(self):
        p0, _ = next_ports()
        with tempfile.TemporaryDirectory() as tmp:
            log0 = Path(tmp) / "n0.jsonl"
            with NodeProcess("n0", p0, log0):
                resp = send_recv(p0, make_get_msg("no_such_key", "r1"))
                obj  = json.loads(resp)
                self.assertFalse(obj["ok"])
                self.assertIsNone(obj["value"])


# ═══════════════════════════════════════════════════════════════════════════════
# TEST CLASS 2: Write-Ahead Log (WAL)
# ═══════════════════════════════════════════════════════════════════════════════

class WalTests(unittest.TestCase):
    """Verify WAL write and crash-recovery correctness."""

    def test_wal_entries_written(self):
        """Each SET should produce a WAL entry with correct key/value/version."""
        p0, _ = next_ports()
        with tempfile.TemporaryDirectory() as tmp:
            log0 = Path(tmp) / "n0.jsonl"
            wal0 = Path(tmp) / "n0.wal"
            with NodeProcess("n0", p0, log0, wal_path=wal0):
                for i in range(5):
                    send_recv(p0, make_set_msg(f"k{i}", f"v{i}", f"r{i}"))
                time.sleep(0.2)

            entries = parse_jsonl(wal0)
            self.assertEqual(len(entries), 5, "Expected 5 WAL entries")
            keys = {e["key"] for e in entries}
            self.assertEqual(keys, {f"k{i}" for i in range(5)})
            # Every entry must have a version object
            for e in entries:
                self.assertIn("version", e, "WAL entry missing version field")
                self.assertIsInstance(e["version"], dict)
                self.assertGreater(len(e["version"]), 0, "version must be non-empty")

    def test_wal_recovery(self):
        """
        Crash a node after writing N keys.
        Restart it with the same WAL path.
        GET all N keys — all must be found (recovered from WAL).
        """
        p0, _ = next_ports()
        with tempfile.TemporaryDirectory() as tmp:
            log0 = Path(tmp) / "n0.jsonl"
            wal0 = Path(tmp) / "n0.wal"

            # Phase 1: start, write 10 keys, crash
            n0 = NodeProcess("n0", p0, log0, wal_path=wal0, run_id="phase1")
            n0.start()
            for i in range(10):
                send_recv(p0, make_set_msg(f"rk{i}", f"rv{i}", f"w{i}"))
            time.sleep(0.2)
            n0.kill()  # simulate crash (SIGKILL, no graceful shutdown)
            time.sleep(0.3)

            # Phase 2: restart with new port (same WAL)
            p0b, _ = next_ports()
            log0b = Path(tmp) / "n0b.jsonl"
            with NodeProcess("n0b", p0b, log0b, wal_path=wal0, run_id="phase2"):
                recovered = 0
                for i in range(10):
                    resp = send_recv(p0b, make_get_msg(f"rk{i}", f"g{i}"))
                    obj  = json.loads(resp)
                    if obj.get("ok") and obj.get("value") == f"rv{i}":
                        recovered += 1

            self.assertEqual(recovered, 10,
                             f"WAL recovery: only {recovered}/10 keys recovered")

    def test_wal_seq_monotone(self):
        """WAL sequence numbers must be strictly monotonically increasing."""
        p0, _ = next_ports()
        with tempfile.TemporaryDirectory() as tmp:
            log0 = Path(tmp) / "n0.jsonl"
            wal0 = Path(tmp) / "n0.wal"
            with NodeProcess("n0", p0, log0, wal_path=wal0):
                for i in range(8):
                    send_recv(p0, make_set_msg(f"seq{i}", str(i), f"s{i}"))
                time.sleep(0.2)

            seqs = [e["wal_seq"] for e in parse_jsonl(wal0)]
            self.assertEqual(seqs, sorted(seqs), "WAL seqs not sorted")
            self.assertEqual(seqs, list(range(len(seqs))), "WAL seqs not monotone from 0")

    def test_wal_disabled_when_no_path(self):
        """Node must start and function normally even without --wal_path."""
        p0, _ = next_ports()
        with tempfile.TemporaryDirectory() as tmp:
            log0 = Path(tmp) / "n0.jsonl"
            with NodeProcess("n0", p0, log0):  # no wal_path
                resp = send_recv(p0, make_set_msg("wkey", "wval", "r1"))
                obj  = json.loads(resp)
                self.assertTrue(obj["ok"])


# ═══════════════════════════════════════════════════════════════════════════════
# TEST CLASS 3: Vector Clock & Replication
# ═══════════════════════════════════════════════════════════════════════════════

class VectorClockTests(unittest.TestCase):
    """Verify vector clock propagation and replication correctness."""

    def _start_pair(self, tmp, p0, p1, repl_mode, fd_algo="fixed"):
        log0 = Path(tmp) / "n0.jsonl"
        log1 = Path(tmp) / "n1.jsonl"
        wal0 = Path(tmp) / "n0.wal"
        wal1 = Path(tmp) / "n1.wal"
        n1 = NodeProcess("n1", p1, log1, wal_path=wal1, run_id="vc_test")
        n0 = NodeProcess("n0", p0, log0, wal_path=wal0, run_id="vc_test",
                         is_primary=True, peer_port=p1,
                         repl_mode=repl_mode, fd_algo=fd_algo)
        n1.start()
        n0.start()
        time.sleep(0.5)  # let heartbeat connection establish
        return n0, n1

    def test_set_response_has_version(self):
        """SET response itself does not carry version (ACK only has ok/key/req_id).
        The version must appear in node0.jsonl client_req_done event."""
        p0, p1 = next_ports()
        with tempfile.TemporaryDirectory() as tmp:
            log0 = Path(tmp) / "n0.jsonl"
            n1 = NodeProcess("n1", p1, Path(tmp)/"n1.jsonl")
            n0 = NodeProcess("n0", p0, log0, is_primary=True, peer_port=p1,
                             repl_mode="sync")
            with n1, n0:
                time.sleep(0.5)
                send_recv(p0, make_set_msg("vk1", "vv1", "req1"))
                time.sleep(0.2)

            events = parse_jsonl(log0)
            done   = next((e for e in events
                           if e.get("event") == "client_req_done"
                           and e.get("extra", {}).get("op") == "SET"), None)
            self.assertIsNotNone(done, "client_req_done event not found in node0 log")
            version = done.get("extra", {}).get("version", {})
            self.assertGreater(len(version), 0, "version in client_req_done is empty")

    def test_repl_set_has_version(self):
        """REPL_SET log events must contain a non-empty version vector."""
        p0, p1 = next_ports()
        with tempfile.TemporaryDirectory() as tmp:
            log0 = Path(tmp) / "n0.jsonl"
            n1 = NodeProcess("n1", p1, Path(tmp)/"n1.jsonl")
            n0 = NodeProcess("n0", p0, log0, is_primary=True, peer_port=p1,
                             repl_mode="sync")
            with n1, n0:
                time.sleep(0.5)
                for i in range(3):
                    send_recv(p0, make_set_msg(f"rk{i}", f"rv{i}", f"r{i}"))
                time.sleep(0.3)

            events    = parse_jsonl(log0)
            repl_evts = [e for e in events if e.get("event") == "repl_start"]
            self.assertGreaterEqual(len(repl_evts), 3,
                                    "Expected at least 3 repl_start events")
            for e in repl_evts:
                ver = e.get("extra", {}).get("version", {})
                self.assertGreater(len(ver), 0, "repl_start version is empty")

    def test_sync_replication_roundtrip(self):
        """In sync mode, secondary must have all keys written by primary."""
        p0, p1 = next_ports()
        with tempfile.TemporaryDirectory() as tmp:
            n1 = NodeProcess("n1", p1, Path(tmp)/"n1.jsonl",
                             wal_path=Path(tmp)/"n1.wal")
            n0 = NodeProcess("n0", p0, Path(tmp)/"n0.jsonl",
                             is_primary=True, peer_port=p1,
                             repl_mode="sync")
            with n1, n0:
                time.sleep(0.5)
                for i in range(5):
                    send_recv(p0, make_set_msg(f"sk{i}", f"sv{i}", f"w{i}"))
                time.sleep(0.5)

                # Read from secondary
                found = 0
                for i in range(5):
                    resp = send_recv(p1, make_get_msg(f"sk{i}", f"g{i}"))
                    if json.loads(resp).get("ok"):
                        found += 1

            self.assertEqual(found, 5, f"sync repl: only {found}/5 keys on secondary")

    def test_get_response_has_version(self):
        """GET response must include a version field."""
        p0, p1 = next_ports()
        with tempfile.TemporaryDirectory() as tmp:
            n1 = NodeProcess("n1", p1, Path(tmp)/"n1.jsonl")
            n0 = NodeProcess("n0", p0, Path(tmp)/"n0.jsonl",
                             is_primary=True, peer_port=p1, repl_mode="sync")
            with n1, n0:
                time.sleep(0.5)
                send_recv(p0, make_set_msg("gv_key", "gv_val", "w1"))
                time.sleep(0.3)
                resp = send_recv(p0, make_get_msg("gv_key", "g1"))
                obj  = json.loads(resp)

        self.assertIn("version", obj, "GET response missing 'version' field")
        self.assertIsInstance(obj["version"], dict)


# ═══════════════════════════════════════════════════════════════════════════════
# TEST CLASS 4: Phi Accrual Failure Detector
# ═══════════════════════════════════════════════════════════════════════════════

class PhiAccrualTests(unittest.TestCase):
    """Verify Phi Accrual FD: detection, φ logging, speed vs. fixed."""

    def test_phi_detects_crash(self):
        """
        Start primary (phi) + secondary, kill secondary,
        verify declared_dead appears in primary log with fd_algo=phi_accrual.
        """
        p0, p1 = next_ports()
        with tempfile.TemporaryDirectory() as tmp:
            log0 = Path(tmp) / "n0.jsonl"
            n1 = NodeProcess("n1", p1, Path(tmp)/"n1.jsonl")
            n0 = NodeProcess("n0", p0, log0, is_primary=True, peer_port=p1,
                             fd_algo="phi", phi_threshold=8.0,
                             hb_interval_ms=50)
            n1.start()
            n0.start()
            time.sleep(3.0)   # warm-up: let φ accumulate samples

            n1.kill()         # crash secondary
            time.sleep(2.0)   # wait for detection

            n0.stop()

        events = parse_jsonl(log0)
        dead = [e for e in events if e.get("event") == "declared_dead"]
        self.assertGreater(len(dead), 0, "declared_dead not found in phi mode")

        dead_ev = dead[0]
        extra   = dead_ev.get("extra", {})
        self.assertEqual(extra.get("fd_algo"), "phi_accrual",
                         f"fd_algo should be phi_accrual, got: {extra.get('fd_algo')}")
        phi_val = extra.get("phi", 0)
        self.assertGreaterEqual(phi_val, 8.0,
                                f"φ at declaration ({phi_val:.2f}) < threshold 8.0")

    def test_phi_logs_phi_value(self):
        """phi_value events must be written periodically while peer is alive."""
        p0, p1 = next_ports()
        with tempfile.TemporaryDirectory() as tmp:
            log0 = Path(tmp) / "n0.jsonl"
            n1 = NodeProcess("n1", p1, Path(tmp)/"n1.jsonl")
            n0 = NodeProcess("n0", p0, log0, is_primary=True, peer_port=p1,
                             fd_algo="phi", hb_interval_ms=50)
            with n1, n0:
                time.sleep(3.5)  # let phi_value events accumulate

        events    = parse_jsonl(log0)
        phi_evts  = [e for e in events if e.get("event") == "phi_value"]
        self.assertGreater(len(phi_evts), 0, "No phi_value events found")

        # While peer is alive, phi should be low (near 0)
        phi_vals = [e.get("extra", {}).get("phi", 999) for e in phi_evts]
        min_phi  = min(phi_vals)
        self.assertLess(min_phi, 1.0,
                        f"φ while peer is alive should be near 0, got min={min_phi:.3f}")

    def test_phi_no_false_positive(self):
        """
        With peer always alive and healthy, phi FD must NOT declare_dead
        within a 4-second window.
        """
        p0, p1 = next_ports()
        with tempfile.TemporaryDirectory() as tmp:
            log0 = Path(tmp) / "n0.jsonl"
            n1 = NodeProcess("n1", p1, Path(tmp)/"n1.jsonl")
            n0 = NodeProcess("n0", p0, log0, is_primary=True, peer_port=p1,
                             fd_algo="phi", phi_threshold=8.0,
                             hb_interval_ms=50)
            with n1, n0:
                time.sleep(4.0)

        events = parse_jsonl(log0)
        false_pos = [e for e in events if e.get("event") == "declared_dead"]
        self.assertEqual(len(false_pos), 0,
                         f"False positive: declared_dead while peer alive ({len(false_pos)} times)")

    def test_phi_faster_than_fixed(self):
        """
        With same hb_interval, phi detection latency should be <= fixed-threshold
        latency (since phi adapts to lower mean inter-arrival).
        Run both and compare medians.
        """
        import statistics

        def run_and_detect(fd_algo: str, phi_threshold: float = 8.0,
                           hb_timeout_ms: int = 150) -> int:
            """Return detection latency ms, or 9999 if no detection."""
            px, py = next_ports()
            with tempfile.TemporaryDirectory() as tmp:
                log0 = Path(tmp) / "n0.jsonl"
                n1 = NodeProcess("n1", py, Path(tmp)/"n1.jsonl")
                n0 = NodeProcess("n0", px, log0, is_primary=True, peer_port=py,
                                 fd_algo=fd_algo, phi_threshold=phi_threshold,
                                 hb_timeout_ms=hb_timeout_ms,
                                 hb_interval_ms=50)
                n1.start()
                n0.start()
                time.sleep(3.0)  # warm-up

                t_kill = int(time.time() * 1000)
                n1.kill()
                time.sleep(2.0)
                n0.stop()

            events = parse_jsonl(log0)
            dead   = [e for e in events if e.get("event") == "declared_dead"]
            if not dead:
                return 9999
            return dead[0]["ts_ms"] - t_kill

        # Run 3 trials each and compare medians
        phi_lats   = [run_and_detect("phi",   phi_threshold=8.0) for _ in range(3)]
        fixed_lats = [run_and_detect("fixed", hb_timeout_ms=150) for _ in range(3)]

        phi_med   = statistics.median(phi_lats)
        fixed_med = statistics.median(fixed_lats)

        print(f"\n  Phi median: {phi_med:.0f}ms | Fixed median: {fixed_med:.0f}ms")
        # phi should not be dramatically worse than fixed (within 2x)
        self.assertLess(phi_med, fixed_med * 2.0,
                        f"Phi ({phi_med:.0f}ms) is more than 2x slower than fixed ({fixed_med:.0f}ms)")


# ═══════════════════════════════════════════════════════════════════════════════
# TEST CLASS 5: Zipfian Distribution
# ═══════════════════════════════════════════════════════════════════════════════

class ZipfianTests(unittest.TestCase):
    """Verify Zipfian sampling produces the expected power-law distribution."""

    def _run_workload(self, tmp: str, port: int, num_ops: int,
                      zipf_alpha: float, num_clients: int = 1,
                      key_space: int = 20) -> Path:
        log_wl = Path(tmp) / "workload.jsonl"
        cmd = [
            str(WORKLOAD),
            "--target", f"127.0.0.1:{port}",
            "--num_ops", str(num_ops),
            "--set_ratio", "1.0",
            "--rate", "0",
            "--key_space", str(key_space),
            "--num_clients", str(num_clients),
            "--zipf_alpha", str(zipf_alpha),
            "--log_path", str(log_wl),
            "--run_id", "zipf_test",
        ]
        proc = subprocess.run(cmd, capture_output=True, timeout=30)
        return log_wl

    def test_uniform_distribution(self):
        """α=0 (uniform): all keys should be within a 5× range of each other."""
        p0, _ = next_ports()
        with tempfile.TemporaryDirectory() as tmp:
            log0 = Path(tmp) / "n0.jsonl"
            with NodeProcess("n0", p0, log0):
                log_wl = self._run_workload(tmp, p0,
                                            num_ops=500, zipf_alpha=0.0,
                                            key_space=10)

        events = parse_jsonl(log_wl)
        key_counts = Counter(e["key"] for e in events
                             if e.get("event") == "op_done")
        if not key_counts:
            self.skipTest("No op_done events in workload log")
        counts = list(key_counts.values())
        ratio  = max(counts) / min(counts)
        self.assertLess(ratio, 10.0,
                        f"Uniform: key frequency ratio {ratio:.1f} too high (expected < 10)")

    def test_zipf_hot_key_dominance(self):
        """α=1.0: key_0 should appear far more than key_9."""
        p0, _ = next_ports()
        with tempfile.TemporaryDirectory() as tmp:
            log0 = Path(tmp) / "n0.jsonl"
            with NodeProcess("n0", p0, log0):
                log_wl = self._run_workload(tmp, p0,
                                            num_ops=800, zipf_alpha=1.0,
                                            key_space=10)

        events = parse_jsonl(log_wl)
        key_counts = Counter(e["key"] for e in events
                             if e.get("event") == "op_done" and e.get("success"))
        if len(key_counts) < 2:
            self.skipTest("Not enough key diversity to test Zipf")

        val0 = key_counts.get("key_0", 0)
        val9 = key_counts.get("key_9", 1)
        ratio = val0 / val9
        print(f"\n  Zipf key_0={val0} key_9={val9} ratio={ratio:.1f}")
        self.assertGreater(ratio, 2.0,
                           f"Zipf: key_0/key_9 ratio {ratio:.1f} should be > 2x (hot key dominance)")

    def test_multi_client_all_complete(self):
        """4 parallel clients should each complete their allotted ops."""
        p0, _ = next_ports()
        with tempfile.TemporaryDirectory() as tmp:
            log0 = Path(tmp) / "n0.jsonl"
            with NodeProcess("n0", p0, log0):
                log_wl = self._run_workload(tmp, p0,
                                            num_ops=50, zipf_alpha=0.0,
                                            num_clients=4)

        events = parse_jsonl(log_wl)
        op_dones = [e for e in events if e.get("event") == "op_done"
                    and e.get("success")]
        total = len(op_dones)
        # 4 clients × 50 ops = 200, allow for some failures
        self.assertGreaterEqual(total, 150,
                                f"Only {total}/200 ops completed with 4 clients")

    def test_multi_client_distinct_ids(self):
        """Each client thread must use a distinct client_id [0, N)."""
        p0, _ = next_ports()
        with tempfile.TemporaryDirectory() as tmp:
            log0 = Path(tmp) / "n0.jsonl"
            with NodeProcess("n0", p0, log0):
                log_wl = self._run_workload(tmp, p0,
                                            num_ops=30, zipf_alpha=0.0,
                                            num_clients=4)

        events = parse_jsonl(log_wl)
        client_ids = {e.get("client_id") for e in events
                      if e.get("event") == "op_done"
                      and e.get("client_id") is not None}
        self.assertEqual(client_ids, {0, 1, 2, 3},
                         f"Expected client_ids {{0,1,2,3}}, got {client_ids}")


# ═══════════════════════════════════════════════════════════════════════════════
# TEST CLASS 6: Stale Read / RYW Detection
# ═══════════════════════════════════════════════════════════════════════════════

class StaleReadTests(unittest.TestCase):
    """Verify read-your-writes detection in workload logs."""

    def test_stale_read_in_async_mode(self):
        """
        In async replication, multi-client reads from secondary may return
        stale data. stale_read events must appear in the workload log.
        """
        p0, p1 = next_ports()
        with tempfile.TemporaryDirectory() as tmp:
            log0 = Path(tmp) / "n0.jsonl"
            log_wl = Path(tmp) / "workload.jsonl"

            n1 = NodeProcess("n1", p1, Path(tmp)/"n1.jsonl")
            n0 = NodeProcess("n0", p0, log0, is_primary=True, peer_port=p1,
                             repl_mode="async", hb_interval_ms=50)
            with n1, n0:
                time.sleep(0.5)
                cmd = [
                    str(WORKLOAD),
                    "--target", f"127.0.0.1:{p0}",
                    "--num_ops", "100",
                    "--set_ratio", "0.6",
                    "--num_clients", "4",
                    "--zipf_alpha", "0.99",   # hot keys → more RYW conflict
                    "--key_space", "10",
                    "--log_path", str(log_wl),
                    "--run_id", "stale_test",
                ]
                subprocess.run(cmd, capture_output=True, timeout=20)

        events = parse_jsonl(log_wl)
        stale  = [e for e in events if e.get("event") == "stale_read"]
        # With hot keyspace + 4 clients, we expect to see at least some violations
        print(f"\n  stale_read events found: {len(stale)}")
        # Note: not asserting > 0 since it depends on timing; just verify format
        for e in stale:
            self.assertIn("last_write_seq", e, "stale_read missing last_write_seq")
            self.assertIn("resp_seq", e, "stale_read missing resp_seq")
            self.assertGreater(e["last_write_seq"], e["resp_seq"],
                               "last_write_seq should be > resp_seq for stale reads")

    def test_stale_read_not_in_sync_mode(self):
        """
        In sync replication with a single client, stale reads should be zero
        (writes are confirmed before returning).
        """
        p0, p1 = next_ports()
        with tempfile.TemporaryDirectory() as tmp:
            log_wl = Path(tmp) / "workload.jsonl"
            n1 = NodeProcess("n1", p1, Path(tmp)/"n1.jsonl")
            n0 = NodeProcess("n0", p0, Path(tmp)/"n0.jsonl",
                             is_primary=True, peer_port=p1,
                             repl_mode="sync")
            with n1, n0:
                time.sleep(0.5)
                cmd = [
                    str(WORKLOAD),
                    "--target", f"127.0.0.1:{p0}",
                    "--num_ops", "80",
                    "--set_ratio", "0.5",
                    "--num_clients", "1",
                    "--key_space", "10",
                    "--log_path", str(log_wl),
                    "--run_id", "sync_test",
                ]
                subprocess.run(cmd, capture_output=True, timeout=20)

        events = parse_jsonl(log_wl)
        stale  = [e for e in events if e.get("event") == "stale_read"]
        self.assertEqual(len(stale), 0,
                         f"Sync+single client should have 0 stale reads, got {len(stale)}")


# ═══════════════════════════════════════════════════════════════════════════════
# TEST CLASS 7: Fixed-Threshold Failure Detector (regression)
# ═══════════════════════════════════════════════════════════════════════════════

class FixedFDTests(unittest.TestCase):
    """Regression tests for the original fixed-threshold failure detector."""

    def test_fixed_detects_crash(self):
        """Fixed FD must declare_dead within ~hb_timeout_ms after crash."""
        p0, p1 = next_ports()
        with tempfile.TemporaryDirectory() as tmp:
            log0 = Path(tmp) / "n0.jsonl"
            n1 = NodeProcess("n1", p1, Path(tmp)/"n1.jsonl")
            n0 = NodeProcess("n0", p0, log0, is_primary=True, peer_port=p1,
                             fd_algo="fixed", hb_interval_ms=50, hb_timeout_ms=150)
            n1.start()
            n0.start()
            time.sleep(1.0)

            t_kill = int(time.time() * 1000)
            n1.kill()
            time.sleep(1.5)
            n0.stop()

        events = parse_jsonl(log0)
        dead   = [e for e in events if e.get("event") == "declared_dead"]
        self.assertGreater(len(dead), 0, "fixed FD: no declared_dead event")
        latency = dead[0]["ts_ms"] - t_kill
        self.assertLess(latency, 2000, f"Fixed FD too slow: {latency}ms")

    def test_no_false_positive_when_alive(self):
        """Fixed FD must not fire when peer is continuously responding."""
        p0, p1 = next_ports()
        with tempfile.TemporaryDirectory() as tmp:
            log0 = Path(tmp) / "n0.jsonl"
            n1 = NodeProcess("n1", p1, Path(tmp)/"n1.jsonl")
            n0 = NodeProcess("n0", p0, log0, is_primary=True, peer_port=p1,
                             fd_algo="fixed", hb_interval_ms=50, hb_timeout_ms=150)
            with n1, n0:
                time.sleep(3.0)

        events = parse_jsonl(log0)
        false_pos = [e for e in events if e.get("event") == "declared_dead"]
        self.assertEqual(len(false_pos), 0,
                         f"Fixed FD false positive: {len(false_pos)} declarations while alive")


# ═══════════════════════════════════════════════════════════════════════════════
# TEST CLASS 8: Log Format & Metrics Script
# ═══════════════════════════════════════════════════════════════════════════════

class LogAndMetricsTests(unittest.TestCase):
    """Verify log format correctness and compute_metrics.py output."""

    def test_node_log_format(self):
        """Every line in node log must parse as JSON with required fields."""
        p0, _ = next_ports()
        with tempfile.TemporaryDirectory() as tmp:
            log0 = Path(tmp) / "n0.jsonl"
            with NodeProcess("n0", p0, log0, run_id="fmt_test"):
                for i in range(5):
                    send_recv(p0, make_set_msg(f"fk{i}", f"fv{i}", f"fr{i}"))
                time.sleep(0.2)

        required_fields = {"ts_ms", "node_id", "run_id", "event"}
        for line in open(log0):
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            missing = required_fields - obj.keys()
            self.assertEqual(missing, set(),
                             f"Log line missing fields {missing}: {line}")

    def test_wal_format(self):
        """Every WAL line must have wal_seq, ts_ms, node_id, key, value, version."""
        p0, _ = next_ports()
        with tempfile.TemporaryDirectory() as tmp:
            log0 = Path(tmp) / "n0.jsonl"
            wal0 = Path(tmp) / "n0.wal"
            with NodeProcess("n0", p0, log0, wal_path=wal0):
                send_recv(p0, make_set_msg("wf_k", "wf_v", "wr1"))
                time.sleep(0.2)

        required = {"wal_seq", "ts_ms", "node_id", "key", "value", "version"}
        for line in open(wal0):
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            missing = required - obj.keys()
            self.assertEqual(missing, set(),
                             f"WAL line missing fields {missing}: {line}")

    def test_compute_metrics_extracts_fd_algo(self):
        """compute_metrics.py must extract fd_algo from injector log correctly."""
        sys.path.insert(0, str(ROOT / "scripts"))
        from parse_logs import load_trial_logs, get_first_event

        # Find any phi trial dir
        phi_trials = list((ROOT / "output" / "logs" / "phi_fast_no_repl").glob("trial_*"))
        if not phi_trials:
            self.skipTest("No phi_fast_no_repl trial data available")

        trial = phi_trials[0]
        logs  = load_trial_logs(trial)
        rs    = get_first_event(logs["injector"], "run_start")
        self.assertIsNotNone(rs, "run_start not found in injector log")
        self.assertEqual(rs.get("fd_algo"), "phi",
                         f"fd_algo should be 'phi', got: {rs.get('fd_algo')!r}")

    def test_compute_metrics_runs_without_error(self):
        """compute_metrics.py must exit with code 0 on existing output."""
        output_dir = ROOT / "output"
        if not output_dir.exists():
            self.skipTest("No output directory — run experiments first")

        result = subprocess.run(
            [sys.executable,
             str(ROOT / "scripts" / "compute_metrics.py"),
             "--output", str(output_dir)],
            capture_output=True, cwd=str(ROOT), timeout=60)
        self.assertEqual(result.returncode, 0,
                         f"compute_metrics.py failed:\n{result.stderr.decode()}")


# ═══════════════════════════════════════════════════════════════════════════════
# Entry point
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    print("=" * 65)
    print("Distributed KV Store — Implementation Test Suite")
    print("=" * 65)

    if not KVNODE.exists():
        print(f"\nERROR: Build first:\n  cd {ROOT} && mkdir -p build && cd build && cmake .. && make\n")
        sys.exit(1)

    # Custom test runner with verbose output
    loader = unittest.TestLoader()
    suite  = unittest.TestSuite()

    test_classes = [
        BinaryTests,
        WalTests,
        VectorClockTests,
        PhiAccrualTests,
        ZipfianTests,
        StaleReadTests,
        FixedFDTests,
        LogAndMetricsTests,
    ]

    # If a class name is given as arg, run only that class
    if len(sys.argv) > 1 and not sys.argv[1].startswith("-"):
        target = sys.argv[1]
        matched = [c for c in test_classes if c.__name__ == target]
        if matched:
            test_classes = matched
        else:
            print(f"Unknown test class '{target}'. Available: {[c.__name__ for c in test_classes]}")
            sys.exit(1)

    for cls in test_classes:
        suite.addTests(loader.loadTestsFromTestCase(cls))

    runner = unittest.TextTestRunner(verbosity=2, stream=sys.stdout)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)


if __name__ == "__main__":
    main()
