# Final-Project-3334-02
# Concurrent Task Dispatcher — Rust
 
A multi-threaded task scheduling simulator built in Rust.
Demonstrates priority-based dispatch, class-reserved worker pools,
two-queue architecture, and clean shutdown.
 
---
 
## Quick Start
 
```bash
# Build
cargo build
 
# Run both experiments (default)
cargo run
 
# Release build (faster simulated execution)
cargo build --release
./target/release/task_dispatcher
```
 
---
 
## Design Summary
 
### Architecture
 
```
  Generator thread
      │
      │  routes by kind
      ├──► CPU Queue (BinaryHeap, priority+FIFO)
      │         ▲
      │         └─── 6 CPU Worker threads
      │
      └──► IO  Queue (BinaryHeap, priority+FIFO)
                ▲
                └─── 4 IO  Worker threads
                          │
                          │  CompletedTask records
                          ▼
                   Collector thread → SharedMetrics (Arc<Mutex<>>)
                          ▲
                   Monitor thread (queue depth sampling)
```
 
**Threads / roles:**
| Thread | Role |
|---|---|
| Generator | Simulates real-time arrivals; sleeps until each task is due, then pushes to the right queue |
| CPU Workers (×6) | Pull from the CPU priority queue; simulate CPU-bound work |
| IO Workers (×4) | Pull from the IO priority queue; simulate IO-bound work |
| Monitor | Samples queue depths every 50 ms → metrics |
| Collector | Drains the completion channel → updates metrics |
 
### Scheduling Policy
 
**Priority-based dispatch with FIFO tie-breaking + class-reserved worker pool.**
 
Each queue is a `BinaryHeap<PrioritizedTask>` ordered by:
1. Priority (1–5, higher wins)
2. Arrival time (earlier wins — FIFO tie-break)
Workers are partitioned by class: CPU workers *only* pull from the CPU queue; IO workers *only* pull from the IO queue. This prevents a CPU-heavy burst from starving IO tasks and vice versa.
 
**Why this policy?**
A single shared queue with one worker pool is simple, but a CPU-heavy workload would fill it and starve IO tasks that could finish in milliseconds. Partitioned queues give each task class a guaranteed share of processing capacity regardless of workload mix.
 
### Shared State
 
| Data | Protection | Reason |
|---|---|---|
| `TaskQueue` inner heap | `Mutex<BinaryHeap>` + `Condvar` | Multiple producers (generator) and multiple consumers (workers) |
| `Metrics` | `Arc<Mutex<Metrics>>` | Shared between collector and monitor threads |
| Worker count | `Arc<AtomicUsize>` | Lock-free decrement on exit; the dispatcher polls to detect full shutdown |
 
Channels (`mpsc`) are used for:
- Generator → main: "all tasks dispatched" signal (`SyncSender<()>`)  
- Workers → Collector: completed task records (unbounded `Sender<CompletedTask>`)  
- Main → Monitor: stop signal
---
 
## Experiments
 
### Experiment A — Balanced Workload
 
**Config:** 600 tasks, 50% CPU / 50% IO, arrivals spread over ~5 s, uniform durations 5–80 ms
 
**Results (Seed 42):**
- Makespan: 3.2 s | Throughput: 187 tasks/s
- Avg wait: 207 ms | Max wait: 2499 ms
- CPU-class avg wait: 11.5 ms | IO-class avg wait: 402 ms
- Pool utilization: 80%
**Interpretation:** With equal numbers of tasks and dedicated worker pools, CPU tasks clear quickly (small queue depth, avg 1.1). IO tasks accumulate more because IO task durations are drawn from the same range but are served by only 4 workers vs 6 for CPU. The fairness gap of ~390 ms is entirely explained by this asymmetry — adding a 5th IO worker would close it. Priority scheduling is visible in per-worker load balance: all workers handle similar task counts, confirming no starvation.
 
---
 
### Experiment B — Stressed Workload
 
**Config:** 700 tasks, 85% CPU / 15% IO, burst of 50 tasks at t=0, durations up to 150 ms
 
**Results (Seed 42):**
- Makespan: 7.7 s | Throughput: 91 tasks/s
- Avg wait: 2953 ms | Max wait: 6841 ms
- CPU-class avg wait: 3369 ms | IO-class avg wait: 622 ms
- Max CPU queue depth: 534
**Interpretation:** The CPU queue backs up dramatically (max depth 534) because 594 CPU tasks arrive faster than 6 workers can drain them. IO workers are under-utilized (workers 6–9 each process only ~27 tasks vs ~100 for CPU workers) — this is exactly the situation the reserved-pool policy is designed to prevent from going the *other* direction. If this were a single shared queue the IO tasks would be buried. With reserved pools, IO tasks clear in an average of 622 ms while CPU tasks are stuck for 3.4 s. To handle this workload better, the policy should be tuned: increase CPU_WORKERS to 8–9, or implement dynamic pool resizing.
 
---
 
## Metrics Collected
 
| Metric | Description |
|---|---|
| Total / CPU / IO completed | Task counts by class |
| Makespan | Wall time from first task to last completion |
| Throughput | Tasks per second |
| Avg / Max wait time | Queue residence time |
| Avg turnaround time | Arrival to completion |
| Pool utilization | Fraction of worker-seconds spent busy |
| Avg / Max queue depth | Both CPU and IO queues |
| CPU-class vs IO-class avg wait | Fairness gap |
| Per-worker breakdown | Tasks done + busy time |
 
---
 
## Project Structure
 
```
src/
  main.rs        — entry point, experiment runner, thread orchestration
  task.rs        — Task struct, TaskKind enum, workload generation
  queue.rs       — TaskQueue (priority BinaryHeap + Condvar), QueuePair
  worker.rs      — worker thread spawn logic
  dispatcher.rs  — generator, monitor, and collector thread spawn logic
  metrics.rs     — Metrics struct, CompletedTask, report printing
Cargo.toml
README.md
REPORT.md
```
 
---
 
## Tool Use Disclosure
 
- **Tools used:** Claude AI (code generation assistant)
- **Kind of help:** Drafted initial module structure, ownership patterns, and metrics report formatting
- **Advice accepted:** Using `BinaryHeap<PrioritizedTask>` with a custom `Ord` impl for the priority queue — cleaner than a hand-rolled sorted Vec
- **Advice rejected / fixed:** An early draft used `Mutex<VecDeque>` with busy-waiting polling loops instead of a `Condvar`. This was replaced with proper `Condvar`-based blocking to avoid wasted CPU cycles in workers waiting for tasks
