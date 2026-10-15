#!/usr/bin/env python3
"""Sample RAPL (package, DRAM) and NVML (GPU) energy on the cluster nodes, and summarize
any time interval of the samples: total, idle-baseline and incremental energy per node.

Sampling runs continuously over a whole benchmark; every run is a time interval cut out of
the samples afterwards. A node that is a virtual machine has no RAPL: its CPU and DRAM energy
is read on its physical host, whose other virtual machines are separated from ours by the CPU
time of each qemu process (``attribution`` in the summary). Its GPU, passed through, is read
inside the virtual machine, where the driver is.

Nodes are listed in nodes.conf, else nodes.conf.example.

The NIC is not measured but modelled, for nodes given its active power (nic_w): with the
byte counters of the node's default-route interface (inside a virtual machine: its own
traffic only),
    P_nic = nic_w * (sent + received bytes/s) / (nic_gbps * 1e9 / 8).
Traffic between Pods of one node and to a local NFS server never reaches the NIC.

Disks are modelled from /proc/diskstats on physical nodes given their datasheet power
(disk=, disk_active_w=, disk_idle_w=): P = idle + (active - idle) * busy fraction (io_ticks),
which counts every request that reaches the device -- the NFS server's included -- and not the
writes absorbed by the page cache. On physical nodes the sampler also records the CPU time of
the whole node and of the kernel NFS server threads (nfsd), so a summary splits the node's
incremental CPU energy into its NFS-server share (``data_service``). Every RAPL target needs the
read-only helper /usr/local/bin/read-rapl with a passwordless sudo rule (see the example).

    rapl.py preflight [--nodes a,b]
    rapl.py start --out DIR [--nodes a,b]     # background sampler, pid in DIR/sampler.pid
    rapl.py mark --out DIR LABEL              # record an instant (e.g. idle-start, idle-end)
    rapl.py stop --out DIR
    rapl.py summarize --out DIR --start T --end T [--baseline T,T ...] [--json FILE]

Times are ISO 8601 (UTC if no offset) or Unix seconds.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
NODES_FILE = Path(os.environ.get("RAPL_NODES_FILE", HERE / "nodes.conf"))
READ_RAPL = "/usr/local/bin/read-rapl"
SSH_OPTIONS = ["-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
               "-o", "ServerAliveInterval=10", "-o", "ServerAliveCountMax=3"]
RAPL_PERIOD_S = 1.0
GPU_PERIOD_MS = 200
# A gap between two samples longer than this marks the interval as partially covered.
MAX_GAP_S = 5.0
# Bin width for the regression that attributes host energy to the qemu processes.
ATTRIBUTION_BIN_S = 10.0
COMPONENT_OF_ZONE = {"package": "cpu", "dram": "ram"}


# ---------------------------------------------------------------------------- nodes

class Node:
    def __init__(self, name: str, ssh: str, host: str, vm: str, options: dict[str, str] | None = None):
        self.name, self.ssh, self.host, self.vm = name, ssh, host, vm
        self.virtual = False  # set by detect()
        options = options or {}
        known = {"nic_w", "nic_gbps", "nic", "disk", "disk_active_w", "disk_idle_w"}
        unknown = set(options) - known
        if unknown:
            raise SystemExit(f"{name}: unknown options {', '.join(sorted(unknown))} ({', '.join(sorted(known))})")
        self.nic_w = float(options["nic_w"]) if "nic_w" in options else None
        self.nic_gbps = float(options.get("nic_gbps", 1))
        self.nic = options.get("nic")  # interface; default: the default route's
        # Block devices (comma-separated) with their active / idle power, one value or one per disk.
        disks = [d for d in options.get("disk", "").split(",") if d]
        def per_disk(key):
            values = [float(v) for v in options.get(key, "").split(",") if v]
            if disks and len(values) not in (1, len(disks)):
                raise SystemExit(f"{name}: {key} needs one value or one per disk")
            return values * len(disks) if len(values) == 1 else values
        active, idle = per_disk("disk_active_w"), per_disk("disk_idle_w")
        if disks and not (active and idle):
            raise SystemExit(f"{name}: disk= needs disk_active_w= and disk_idle_w=")
        self.disks = {disk: (a, i) for disk, a, i in zip(disks, active, idle)}

    @property
    def rapl_target(self) -> str:
        return self.host if self.virtual else self.ssh


def load_nodes(selected: list[str] | None = None) -> list[Node]:
    path = NODES_FILE if NODES_FILE.exists() else HERE / "nodes.conf.example"
    nodes = []
    for line in path.read_text(encoding="utf-8").splitlines():
        fields = line.split("#", 1)[0].split()
        if not fields:
            continue
        if len(fields) < 4 or any("=" not in field for field in fields[4:]):
            raise SystemExit(f"{path}: expected 'node ssh host vm [key=value ...]', got: {line.strip()}")
        options = dict(field.split("=", 1) for field in fields[4:])
        nodes.append(Node(*(None if value == "-" else value for value in fields[:4]), options))
    if selected:
        unknown = set(selected) - {node.name for node in nodes}
        if unknown:
            raise SystemExit(f"nodes not in {path}: {', '.join(sorted(unknown))}")
        nodes = [node for node in nodes if node.name in selected]
    return nodes


def command(target: str | None, script: str) -> list[str]:
    """Run a shell script locally (target 'local') or over ssh."""
    if target in (None, "local"):
        return ["sh", "-c", script]
    return ["ssh", *SSH_OPTIONS, target, script]


def check_output(target: str | None, script: str, timeout: float = 30) -> str:
    result = subprocess.run(command(target, script), capture_output=True, text=True, timeout=timeout)
    if result.returncode != 0:
        raise RuntimeError((result.stderr or result.stdout).strip() or f"exit {result.returncode}")
    return result.stdout


def detect(node: Node) -> None:
    """A node is virtual if systemd-detect-virt says so; it then needs a host and a vm name."""
    virt = check_output(node.ssh, "systemd-detect-virt 2>/dev/null || true").strip() or "none"
    node.virtual = virt != "none"
    if node.virtual and not (node.host and node.vm):
        raise RuntimeError(f"{node.name} is a virtual machine ({virt}) but nodes.conf gives no host/vm "
                           "to read its RAPL on")


# Remote loop on the RAPL target: one 'R' line per zone and, for virtual nodes, one 'Q' line
# per qemu process (utime + stime in clock ticks, guest name from its command line).
RAPL_LOOP = """
tck=$(getconf CLK_TCK)
while :; do
  out=$(sudo -n {read_rapl}) || exit 3
  echo "$out" | sed 's/^/R /'
  if [ -n "{qemu}" ]; then
    t=$(date +%s.%N)
    for p in $(pgrep qemu-system); do
      s=$(cat /proc/$p/stat 2>/dev/null) || continue
      n=$(tr '\\0' '\\n' < /proc/$p/cmdline 2>/dev/null | sed -n '/^-name$/{{n;p;q}}' | sed 's/^guest=//; s/,.*//')
      echo "$s" | sed 's/^.*) //' | awk -v t="$t" -v p="$p" -v n="${{n:-?}}" -v k="$tck" '{{print "Q", t, p, n, $12 + $13, k}}'
    done
  fi
  sleep {period}
done
"""

# On the node itself: cumulative bytes of one interface, one 'N' line per second.
NIC_SAMPLE = """
i={nic}
[ -n "$i" ] || i=$(ip route get 1.1.1.1 2>/dev/null | sed -n 's/.* dev \\([^ ]*\\).*/\\1/p')
[ -r /sys/class/net/$i/statistics/rx_bytes ] || {{ echo "no interface statistics for '$i'" >&2; exit 3; }}
sample() {{ echo "N $(date +%s.%N) $i $(cat /sys/class/net/$i/statistics/rx_bytes) $(cat /sys/class/net/$i/statistics/tx_bytes)"; }}
"""
NIC_LOOP = NIC_SAMPLE + """
while :; do sample; sleep {period}; done
"""

# On a physical node: whole-node CPU ticks ('C' busy total), kernel NFS server ticks ('F'),
# and per configured disk the sectors read/written and busy milliseconds ('D').
SYS_LOOP = """
tck=$(getconf CLK_TCK)
while :; do
  t=$(date +%s.%N)
  awk -v t="$t" '/^cpu /{{b=$2+$3+$4+$7+$8+$9; print "C", t, b, b+$5+$6; exit}}' /proc/stat
  n=0
  for p in $(pgrep -x nfsd); do
    s=$(cat /proc/$p/stat 2>/dev/null) || continue
    n=$((n + $(echo "$s" | sed 's/^.*) //' | awk '{{print $12 + $13}}')))
  done
  echo "F $t $n $tck"
  for d in {disks}; do awk -v t="$t" -v d="$d" '$3 == d {{print "D", t, d, $6, $10, $13}}' /proc/diskstats; done
  sleep {period}
done
"""

GPU_LOOP = ("command -v nvidia-smi >/dev/null || exit 0; "
            "f=power.draw.instant; nvidia-smi --query-gpu=$f --format=csv >/dev/null 2>&1 || f=power.draw; "
            "exec stdbuf -oL nvidia-smi --query-gpu=index,name,$f --format=csv,noheader,nounits "
            f"-lms {GPU_PERIOD_MS}")


def preflight(nodes: list[Node]) -> bool:
    ok = True
    for node in nodes:
        try:
            detect(node)
            zones = [line.split()[2] for line in check_output(node.rapl_target, f"sudo -n {READ_RAPL}").splitlines()]
            gpus = check_output(node.ssh, "command -v nvidia-smi >/dev/null && "
                                "nvidia-smi --query-gpu=name --format=csv,noheader || true").split("\n")
            gpus = [gpu.strip() for gpu in gpus if gpu.strip()]
            note = ""
            if node.virtual:
                names = check_output(node.host, "for p in $(pgrep qemu-system); do tr '\\0' '\\n' < /proc/$p/cmdline "
                                     "| sed -n '/^-name$/{n;p;q}' | sed 's/^guest=//; s/,.*//'; done").split()
                if node.vm not in names:
                    raise RuntimeError(f"no qemu process named {node.vm} on {node.host} (found: {', '.join(names)})")
                note = f"; host {node.host} runs {len(names)} VMs: {', '.join(names)}"
            if not node.virtual:
                present = set(check_output(node.ssh, "awk '{print $3}' /proc/diskstats").split())
                missing = set(node.disks) - present
                if missing:
                    raise RuntimeError(f"disks not in /proc/diskstats: {', '.join(sorted(missing))}")
                nfsd = len(check_output(node.ssh, "pgrep -x nfsd || true").split())
                note += f"; nfsd threads={nfsd}" + (f"; disks {','.join(node.disks)}" if node.disks else "")
            if node.nic_w is not None:
                line = check_output(node.ssh, NIC_SAMPLE.format(nic=node.nic or "") + "sample\n")
                note += f"; NIC model {line.split()[2]} {node.nic_w} W at {node.nic_gbps:g} Gb/s"
            print(f"[ok] {node.name}: {'VM, RAPL on ' + node.host if node.virtual else 'RAPL local'} "
                  f"zones={','.join(zones)} gpu={','.join(gpus) or 'none'}{note}")
        except (RuntimeError, subprocess.TimeoutExpired) as error:
            ok = False
            print(f"[FAIL] {node.name}: {error}", file=sys.stderr)
    return ok


# ---------------------------------------------------------------------------- sampling

class Stream(threading.Thread):
    """Keep one sampling command alive and append its parsed lines to TSV files."""

    def __init__(self, out: Path, node: Node, kind: str, target: str, script: str, stop: threading.Event):
        super().__init__(daemon=True)
        self.out, self.node, self.kind, self.target, self.script, self.stop = out, node, kind, target, script, stop
        self.process: subprocess.Popen | None = None

    def event(self, message: str) -> None:
        with (self.out / "events.tsv").open("a", encoding="utf-8") as stream:
            stream.write(f"{time.time():.3f}\t{self.node.name}\t{self.kind}\t{message}\n")

    def run(self) -> None:
        files = {}
        try:
            while not self.stop.is_set():
                self.event("start")
                self.process = subprocess.Popen(command(self.target, self.script), stdout=subprocess.PIPE,
                                                stderr=subprocess.PIPE, text=True, bufsize=1)
                for line in self.process.stdout:
                    self.handle(line if self.kind == "gpu" else line.split(), files)
                code = self.process.wait()
                error = (self.process.stderr.read() or "").strip().replace("\n", " ")[:300]
                if self.stop.is_set():
                    break
                if self.kind == "gpu" and code == 0:
                    self.event("no nvidia-smi; GPU not sampled")
                    break
                self.event(f"exit {code}: {error}; restart in 5 s")
                self.stop.wait(5)
        finally:
            for stream in files.values():
                stream.close()

    def write(self, files: dict, name: str, header: str, row: str) -> None:
        if name not in files:
            path = self.out / name
            new = not path.exists()
            files[name] = path.open("a", encoding="utf-8", buffering=1)
            if new:
                files[name].write(header + "\n")
        files[name].write(row + "\n")

    def handle(self, line, files: dict) -> None:
        if self.kind == "gpu":
            fields = [field.strip() for field in line.split(",")]
            if len(fields) == 3:
                self.write(files, f"gpu-{self.node.name}.tsv", "t\tindex\tname\tpower_w",
                           f"{time.time():.3f}\t{fields[0]}\t{fields[1]}\t{fields[2]}")
        elif line and line[0] == "R" and len(line) == 6:
            self.write(files, f"rapl-{self.node.name}.tsv", "t\tzone\tname\tenergy_uj\tmax_energy_range_uj",
                       "\t".join(line[1:]))
        elif line and line[0] == "Q" and len(line) == 6:
            self.write(files, f"qemu-{self.node.name}.tsv", "t\tpid\tvm\tcpu_ticks\tclk_tck", "\t".join(line[1:]))
        elif line and line[0] == "C" and len(line) == 4:
            self.write(files, f"cpu-{self.node.name}.tsv", "t\tbusy_ticks\ttotal_ticks", "\t".join(line[1:]))
        elif line and line[0] == "F" and len(line) == 4:
            self.write(files, f"nfsd-{self.node.name}.tsv", "t\tcpu_ticks\tclk_tck", "\t".join(line[1:]))
        elif line and line[0] == "D" and len(line) == 6:
            self.write(files, f"disk-{self.node.name}.tsv", "t\tdisk\tsectors_read\tsectors_written\tbusy_ms",
                       "\t".join(line[1:]))
        elif line and line[0] == "N" and len(line) == 5:
            self.write(files, f"nic-{self.node.name}.tsv", "t\tinterface\trx_bytes\ttx_bytes", "\t".join(line[1:]))

    def terminate(self) -> None:
        if self.process and self.process.poll() is None:
            self.process.terminate()


def run_sampler(out: Path, nodes: list[Node]) -> None:
    out.mkdir(parents=True, exist_ok=True)
    for node in nodes:
        detect(node)
    (out / "sampler.json").write_text(json.dumps({
        "started_at": time.time(),
        "nodes": {node.name: {"virtual": node.virtual, "rapl_target": node.rapl_target,
                              "gpu_target": node.ssh, "vm": node.vm if node.virtual else None,
                              "nic_w": node.nic_w, "nic_gbps": node.nic_gbps,
                              "disks": {d: {"active_w": a, "idle_w": i} for d, (a, i) in node.disks.items()}}
                  for node in nodes},
    }, indent=2), encoding="utf-8")
    stop = threading.Event()
    streams = []
    for node in nodes:
        rapl_script = RAPL_LOOP.format(read_rapl=READ_RAPL, qemu="1" if node.virtual else "", period=RAPL_PERIOD_S)
        streams.append(Stream(out, node, "rapl", node.rapl_target, rapl_script, stop))
        streams.append(Stream(out, node, "gpu", node.ssh, GPU_LOOP, stop))
        if not node.virtual:
            sys_script = SYS_LOOP.format(disks=" ".join(node.disks), period=RAPL_PERIOD_S)
            streams.append(Stream(out, node, "sys", node.ssh, sys_script, stop))
        if node.nic_w is not None:
            nic_script = NIC_LOOP.format(nic=node.nic or "", period=RAPL_PERIOD_S)
            streams.append(Stream(out, node, "nic", node.ssh, nic_script, stop))

    def shutdown(*_):
        stop.set()
        for stream in streams:
            stream.terminate()
    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    for stream in streams:
        stream.start()
    while not stop.is_set():
        stop.wait(1)
    for stream in streams:
        stream.join(timeout=10)


def start(out: Path, selected: list[str] | None) -> None:
    out.mkdir(parents=True, exist_ok=True)
    pidfile = out / "sampler.pid"
    if pidfile.exists() and alive(int(pidfile.read_text())):
        print(f"sampler already running (pid {pidfile.read_text().strip()})")
        return
    args = [sys.executable, str(Path(__file__).resolve()), "run", "--out", str(out)]
    if selected:
        args += ["--nodes", ",".join(selected)]
    with (out / "sampler.log").open("a") as log:
        process = subprocess.Popen(args, stdout=log, stderr=log, stdin=subprocess.DEVNULL, start_new_session=True)
    pidfile.write_text(str(process.pid))
    time.sleep(3)
    if process.poll() is not None:
        raise SystemExit(f"sampler exited at start; see {out / 'sampler.log'}")
    print(f"sampler started (pid {process.pid}), samples in {out}")


def alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def stop_sampler(out: Path) -> None:
    pidfile = out / "sampler.pid"
    if not pidfile.exists():
        print("no sampler.pid")
        return
    pid = int(pidfile.read_text())
    if alive(pid):
        os.kill(pid, signal.SIGTERM)
        for _ in range(30):
            if not alive(pid):
                break
            time.sleep(0.5)
    pidfile.unlink()
    print(f"sampler {pid} stopped")


def mark(out: Path, label: str) -> None:
    with (out / "marks.tsv").open("a", encoding="utf-8") as stream:
        stream.write(f"{time.time():.3f}\t{label}\n")


# ---------------------------------------------------------------------------- summary

def parse_time(value: str | float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        moment = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=timezone.utc)
        return moment.timestamp()


def read_tsv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    lines = path.read_text(encoding="utf-8").splitlines()
    header = lines[0].split("\t")
    return [dict(zip(header, line.split("\t"))) for line in lines[1:] if line.count("\t") == len(header) - 1]


def cumulative(samples: list[tuple[float, float, float]]) -> list[tuple[float, float]]:
    """(t, raw counter, wrap range) -> (t, unwrapped cumulative joules)."""
    series, total, previous = [], 0.0, None
    for t, raw, wrap in samples:
        if previous is not None:
            delta = raw - previous
            total += (delta + wrap if delta < 0 else delta) / 1e6
        series.append((t, total))
        previous = raw
    return series


def value_at(series: list[tuple[float, float]], t: float) -> float | None:
    """Linear interpolation of a cumulative series; None outside its range."""
    if not series or t < series[0][0] or t > series[-1][0]:
        return None
    low, high = 0, len(series) - 1
    while high - low > 1:
        middle = (low + high) // 2
        if series[middle][0] <= t:
            low = middle
        else:
            high = middle
    (t0, v0), (t1, v1) = series[low], series[high]
    return v0 if t1 == t0 else v0 + (v1 - v0) * (t - t0) / (t1 - t0)


def max_gap(times: list[float], start: float, end: float) -> float:
    inside = [start] + [t for t in times if start < t < end] + [end]
    return max(b - a for a, b in zip(inside, inside[1:]))


def energy_series(out: Path, node: str, info: dict | None = None) -> dict[str, list[tuple[float, float]]]:
    """Per component, the cumulative energy (J) of the node: RAPL zones, integrated GPU power
    and, with nic_w in ``info`` (sampler.json), the modelled NIC energy."""
    zones: dict[str, list[tuple[float, float, float]]] = {}
    names: dict[str, str] = {}
    for row in read_tsv(out / f"rapl-{node}.tsv"):
        zones.setdefault(row["zone"], []).append(
            (float(row["t"]), float(row["energy_uj"]), float(row["max_energy_range_uj"])))
        names[row["zone"]] = row["name"]
    components: dict[str, list[list[tuple[float, float]]]] = {}
    for zone, samples in zones.items():
        component = COMPONENT_OF_ZONE.get(names[zone].split("-")[0], "other")
        components.setdefault(component, []).append(cumulative(sorted(samples)))
    gpus: dict[str, list[tuple[float, float]]] = {}
    for row in read_tsv(out / f"gpu-{node}.tsv"):
        try:
            gpus.setdefault(row["index"], []).append((float(row["t"]), float(row["power_w"])))
        except ValueError:  # [N/A]
            continue
    for samples in gpus.values():
        samples.sort()
        series, total = [], 0.0
        for i, (t, power) in enumerate(samples):
            if i:
                t0, p0 = samples[i - 1]
                # Trapezoid, bridging gaps too (they are reported by max_gap_s).
                total += (t - t0) * (power + p0) / 2
            series.append((t, total))
        components.setdefault("gpu", []).append(series)
    if info and info.get("nic_w") is not None:
        # E = nic_w * bytes / (bytes/s at line rate); a counter reset adds nothing.
        joules_per_byte = info["nic_w"] / (info["nic_gbps"] * 1e9 / 8)
        series, total, previous = [], 0.0, None
        for row in read_tsv(out / f"nic-{node}.tsv"):
            count = float(row["rx_bytes"]) + float(row["tx_bytes"])
            if previous is not None and count >= previous:
                total += (count - previous) * joules_per_byte
            previous = count
            series.append((float(row["t"]), total))
        if series:
            components["nic"] = [series]
    disks = (info or {}).get("disks") or {}
    busy: dict[str, list[tuple[float, float]]] = {}
    for row in read_tsv(out / f"disk-{node}.tsv"):
        if row["disk"] in disks:
            busy.setdefault(row["disk"], []).append((float(row["t"]), float(row["busy_ms"]) / 1000))
    for disk, samples in busy.items():
        # E(t) = idle * elapsed + (active - idle) * busy seconds, from the first sample on.
        active, idle = disks[disk]["active_w"], disks[disk]["idle_w"]
        samples.sort()
        t0, b0 = samples[0]
        components.setdefault("storage", []).append(
            [(t, idle * (t - t0) + (active - idle) * max(0.0, b - b0)) for t, b in samples])
    # Sum several sockets / GPUs of one component on the union of their time stamps.
    merged = {}
    for component, parts in components.items():
        if len(parts) == 1:
            merged[component] = parts[0]
            continue
        times = sorted({t for part in parts for t, _ in part})
        merged[component] = [(t, sum(value_at(part, t) or 0.0 for part in parts)) for t in times
                             if all(value_at(part, t) is not None for part in parts)]
    return merged


def interval_energy(series: dict[str, list[tuple[float, float]]], start: float, end: float):
    energies, gaps = {}, {}
    for component, values in series.items():
        a, b = value_at(values, start), value_at(values, end)
        gaps[component] = max_gap([t for t, _ in values], start, end)
        if a is not None and b is not None:
            energies[component] = b - a
    return energies, gaps


def qemu_usage(out: Path, node: str):
    """Cumulative CPU seconds of every qemu process, keyed by (pid, guest name)."""
    ticks: dict[tuple[str, str], list[tuple[float, float]]] = {}
    for row in read_tsv(out / f"qemu-{node}.tsv"):
        ticks.setdefault((row["pid"], row["vm"]), []).append(
            (float(row["t"]), float(row["cpu_ticks"]) / float(row["clk_tck"])))
    return {key: sorted(values) for key, values in ticks.items()}


def cores_between(ticks: dict, vm: str, start: float, end: float) -> tuple[float, float] | None:
    ours = others = 0.0
    found = False
    for (_, name), series in ticks.items():
        a, b = value_at(series, start), value_at(series, end)
        if a is None or b is None:
            continue
        found = found or name == vm
        if name == vm:
            ours += (b - a) / (end - start)
        else:
            others += (b - a) / (end - start)
    return (ours, others) if found else None


def ols(xs: list[list[float]], ys: list[float]) -> tuple[list[float], float]:
    """Least squares y = c0 + c.x via normal equations; returns (coefficients, R^2)."""
    rows = [[1.0, *x] for x in xs]
    k = len(rows[0])
    a = [[sum(r[i] * r[j] for r in rows) for j in range(k)] for i in range(k)]
    b = [sum(r[i] * y for r, y in zip(rows, ys)) for i in range(k)]
    for i in range(k):  # Gauss-Jordan
        pivot = max(range(i, k), key=lambda r: abs(a[r][i]))
        a[i], a[pivot], b[i], b[pivot] = a[pivot], a[i], b[pivot], b[i]
        if abs(a[i][i]) < 1e-12:
            raise ValueError("singular regression (no load variation)")
        for r in range(k):
            if r != i:
                factor = a[r][i] / a[i][i]
                a[r] = [x - factor * y for x, y in zip(a[r], a[i])]
                b[r] -= factor * b[i]
    coefficients = [b[i] / a[i][i] for i in range(k)]
    mean = sum(ys) / len(ys)
    fitted = [sum(c * v for c, v in zip(coefficients, r)) for r in rows]
    total = sum((y - mean) ** 2 for y in ys)
    residual = sum((y - f) ** 2 for y, f in zip(ys, fitted))
    return coefficients, (1 - residual / total) if total else 0.0


def attribute(series, ticks, vm: str, run: tuple[float, float], baselines: list[tuple[float, float]],
              idle_ours: float) -> dict:
    """Split the host's CPU+DRAM energy between our VM and the other qemu processes.

    On ATTRIBUTION_BIN_S bins of the baseline and run intervals, fit
        P_component = a + b_ours * cores_ours + b_others * cores_others
    and attribute b_ours * (cores_ours - idle cores_ours) * duration to our VM."""
    bins = []
    for start, end in [*baselines, run]:
        t = start
        while t + ATTRIBUTION_BIN_S <= end:
            usage = cores_between(ticks, vm, t, t + ATTRIBUTION_BIN_S)
            energy, _ = interval_energy(series, t, t + ATTRIBUTION_BIN_S)
            if usage and "cpu" in energy:
                bins.append((t, usage, {c: e / ATTRIBUTION_BIN_S for c, e in energy.items()}))
            t += ATTRIBUTION_BIN_S
    result = {"bins": len(bins), "bin_seconds": ATTRIBUTION_BIN_S, "components": {}}
    usage = cores_between(ticks, vm, *run)
    if usage is None or len(bins) < 10:
        result["error"] = "not enough qemu samples"
        return result
    duration = run[1] - run[0]
    result["vm_cores_run"], result["other_vm_cores_run"] = round(usage[0], 3), round(usage[1], 3)
    base = [cores_between(ticks, vm, *b) for b in baselines]
    base = [b for b in base if b]
    if base:
        result["other_vm_cores_baseline"] = round(sum(b[1] for b in base) / len(base), 3)
    for component in ("cpu", "ram"):
        points = [(u, p[component]) for _, u, p in bins if component in p]
        try:
            (a, b_ours, b_others), r2 = ols([[u[0], u[1]] for u, _ in points], [p for _, p in points])
        except (ValueError, ZeroDivisionError) as error:
            result["components"][component] = {"error": str(error)}
            continue
        result["components"][component] = {
            "intercept_w": round(a, 3), "w_per_vm_core": round(b_ours, 3),
            "w_per_other_core": round(b_others, 3), "r2": round(r2, 3),
            "attributed_incremental_j": round(max(0.0, b_ours * (usage[0] - idle_ours) * duration), 3),
        }
    return result


def summarize(out: Path, start: float, end: float, baselines: list[tuple[float, float]]) -> dict:
    meta = json.loads((out / "sampler.json").read_text(encoding="utf-8"))
    summary = {"start": start, "end": end, "duration_s": round(end - start, 3), "samples_dir": str(out),
               "baselines": [list(b) for b in baselines], "nodes": {}}
    status = "complete"
    totals: dict[str, float] = {}
    incremental: dict[str, float] = {}
    for node, info in meta["nodes"].items():
        series = energy_series(out, node, info)
        energies, gaps = interval_energy(series, start, end)
        node_summary = {"virtual": info["virtual"], "total_j": {c: round(e, 3) for c, e in energies.items()},
                        "max_gap_s": {c: round(g, 3) for c, g in gaps.items()}}
        missing = {"cpu", "ram"} - set(energies)
        if missing or any(g > MAX_GAP_S for g in gaps.values()):
            status = "partial"
            node_summary["coverage"] = "partial" + (f" (missing {','.join(sorted(missing))})" if missing else "")
        idle_power, idle_spread = {}, {}
        for component in energies:
            powers = []
            for b in baselines:
                for t in frange(b[0], b[1] - ATTRIBUTION_BIN_S, ATTRIBUTION_BIN_S):
                    e, _ = interval_energy({component: series[component]}, t, t + ATTRIBUTION_BIN_S)
                    if component in e:
                        powers.append(e[component] / ATTRIBUTION_BIN_S)
            if powers:
                mean = sum(powers) / len(powers)
                idle_power[component] = round(mean, 3)
                idle_spread[component] = round(math.sqrt(sum((p - mean) ** 2 for p in powers) / len(powers)), 3)
        node_summary["idle_power_w"], node_summary["idle_power_std_w"] = idle_power, idle_spread
        node_incremental = {c: e - idle_power[c] * (end - start) for c, e in energies.items() if c in idle_power}
        if info["virtual"] and baselines:
            ticks = qemu_usage(out, node)
            idle_cores = [cores_between(ticks, info["vm"], *b) for b in baselines]
            idle_ours = sum(c[0] for c in idle_cores if c) / max(1, sum(1 for c in idle_cores if c))
            attribution = attribute(series, ticks, info["vm"], (start, end), baselines, idle_ours)
            node_summary["attribution"] = attribution
            node_summary["host_incremental_j"] = {c: round(node_incremental[c], 3)
                                                  for c in ("cpu", "ram") if c in node_incremental}
            # Our VM's CPU/DRAM share replaces the whole host's increment.
            for component in ("cpu", "ram"):
                share = attribution.get("components", {}).get(component, {}).get("attributed_incremental_j")
                if share is None:
                    node_incremental.pop(component, None)
                    status = "partial"
                else:
                    node_incremental[component] = share
        node_summary["incremental_j"] = {c: round(e, 3) for c, e in node_incremental.items()}
        service = data_service(out, node, info, start, end, baselines, node_incremental)
        if service:
            node_summary["data_service"] = service
        summary["nodes"][node] = node_summary
        for component, value in energies.items():
            totals[component] = totals.get(component, 0.0) + value
        for component, value in node_incremental.items():
            incremental[component] = incremental.get(component, 0.0) + value
    summary["measurement_status"] = status if baselines else f"{status} (no idle baseline)"
    modelled = {}
    if any(info.get("nic_w") is not None for info in meta["nodes"].values()):
        modelled["nic"] = "nic_w * (rx + tx bytes/s) / line rate, not measured"
    if any(info.get("disks") for info in meta["nodes"].values()):
        modelled["storage"] = "idle_w + (active_w - idle_w) * busy fraction (diskstats io_ticks), not measured"
    if modelled:
        summary["modelled_components"] = modelled
    summary["by_component_j"] = {c: round(v, 3) for c, v in totals.items()}
    summary["total_energy_j"] = round(sum(totals.values()), 3)
    summary["incremental_by_component_j"] = {c: round(v, 3) for c, v in incremental.items()}
    summary["incremental_energy_j"] = round(sum(incremental.values()), 3) if baselines else None
    return summary


def counter_series(rows: list[dict[str, str]], column: str) -> list[tuple[float, float]]:
    return sorted((float(row["t"]), float(row[column])) for row in rows)


def increase(series: list[tuple[float, float]], start: float, end: float) -> float | None:
    a, b = value_at(series, start), value_at(series, end)
    return None if a is None or b is None else b - a


def data_service(out: Path, node: str, info: dict, start: float, end: float,
                 baselines: list[tuple[float, float]], incremental: dict[str, float]) -> dict:
    """The node's work for data exchange: kernel NFS server CPU time and its share of the
    incremental CPU energy (by CPU time above idle), disk traffic, and the modelled
    storage and NIC increments."""
    cpu_rows = read_tsv(out / f"cpu-{node}.tsv")
    nfsd_rows = read_tsv(out / f"nfsd-{node}.tsv")
    if not cpu_rows or not nfsd_rows:
        return {}
    busy, nfsd = counter_series(cpu_rows, "busy_ticks"), counter_series(nfsd_rows, "cpu_ticks")
    tck = float(nfsd_rows[0]["clk_tck"])
    duration = end - start
    busy_run, nfsd_run = increase(busy, start, end), increase(nfsd, start, end)
    if busy_run is None or nfsd_run is None:
        return {"error": "no CPU samples over the interval"}

    def idle_rate(series):
        rates = [increase(series, *b) / (b[1] - b[0]) for b in baselines if increase(series, *b) is not None]
        return sum(rates) / len(rates) if rates else 0.0

    busy_excess = busy_run - idle_rate(busy) * duration
    nfsd_excess = nfsd_run - idle_rate(nfsd) * duration
    share = min(1.0, max(0.0, nfsd_excess / busy_excess)) if busy_excess > 0 else 0.0
    result = {
        "nfsd_cpu_seconds": round(nfsd_run / tck, 3),
        "nfsd_share_of_incremental_cpu_time": round(share, 4),
        "nfsd_cpu_j": round(share * incremental["cpu"], 3) if "cpu" in incremental and baselines else None,
    }
    disks = info.get("disks") or {}
    rows = [row for row in read_tsv(out / f"disk-{node}.tsv") if row["disk"] in disks]
    if rows:
        read = written = 0.0
        for disk in disks:
            mine = [row for row in rows if row["disk"] == disk]
            read += increase(counter_series(mine, "sectors_read"), start, end) or 0.0
            written += increase(counter_series(mine, "sectors_written"), start, end) or 0.0
        result["disk_read_gb"] = round(read * 512 / 1e9, 3)
        result["disk_written_gb"] = round(written * 512 / 1e9, 3)
    for component in ("storage", "nic"):
        if component in incremental:
            result[f"{component}_incremental_j"] = round(incremental[component], 3)
    return result


def frange(start: float, stop: float, step: float):
    while start <= stop:
        yield start
        start += step


# ---------------------------------------------------------------------------- CLI

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="action", required=True)
    for name in ("preflight", "run", "start"):
        p = sub.add_parser(name)
        p.add_argument("--nodes", default="", help="comma-separated node names (default: all in nodes.conf)")
        if name != "preflight":
            p.add_argument("--out", type=Path, required=True)
    p = sub.add_parser("stop")
    p.add_argument("--out", type=Path, required=True)
    p = sub.add_parser("mark")
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("label")
    p = sub.add_parser("summarize")
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    p.add_argument("--baseline", action="append", default=[], help="START,END of an idle interval")
    p.add_argument("--json", type=Path, help="write the summary here (default: stdout)")
    args = parser.parse_args()

    selected = [n for n in getattr(args, "nodes", "").split(",") if n]
    if args.action == "preflight":
        sys.exit(0 if preflight(load_nodes(selected)) else 1)
    if args.action == "run":
        run_sampler(args.out, load_nodes(selected))
    elif args.action == "start":
        start(args.out, selected)
    elif args.action == "stop":
        stop_sampler(args.out)
    elif args.action == "mark":
        mark(args.out, args.label)
    elif args.action == "summarize":
        baselines = [tuple(parse_time(v) for v in b.split(",", 1)) for b in args.baseline]
        summary = summarize(args.out, parse_time(args.start), parse_time(args.end), baselines)
        text = json.dumps(summary, indent=2)
        if args.json:
            args.json.parent.mkdir(parents=True, exist_ok=True)
            args.json.write_text(text, encoding="utf-8")
        else:
            print(text)


if __name__ == "__main__":
    main()
