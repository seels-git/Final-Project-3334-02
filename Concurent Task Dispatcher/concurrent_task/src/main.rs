// main.rs — Concurrent Task Dispatcher in Rust
//
// Thread roles
// ────────────
//   generator   — simulates real-time task arrivals, pushes to queues
//   cpu workers — pull from the CPU priority queue, simulate execution
//   io  workers — pull from the IO  priority queue, simulate execution
//   monitor     — periodically samples queue depths
//   collector   — drains the completion channel, updates metrics
//
// Scheduling policy
// ─────────────────
//   Priority-based dispatch with FIFO tie-breaking.
//   Workers are partitioned: 6 CPU workers + 4 IO workers (configurable).
//   This reservation ensures IO tasks are never blocked by CPU-heavy
//   workloads and vice versa. Within each queue, tasks are ordered by
//   priority (1–5, higher wins) with ties broken by arrival time (FIFO).
//
// Shared-state ownership
// ──────────────────────
//   QueuePair — Arc<(Mutex<BinaryHeap>, Condvar)> inside each TaskQueue
//   Metrics   — Arc<Mutex<Metrics>>
//   active_workers counter — Arc<AtomicUsize>

mod dispatcher;
mod metrics;
mod queue;
mod task;
mod worker;

use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use rand::SeedableRng;
use rand::rngs::StdRng;

use task::WorkloadConfig;

// ────────────────────────────────────────────────────────────────────────────
// Configuration constants
// ────────────────────────────────────────────────────────────────────────────

const SEED: u64 = 42;

// Worker pool split: must total between 4 and 12 per the rubric.
const CPU_WORKERS: usize = 6;
const IO_WORKERS: usize = 4;
const TOTAL_WORKERS: usize = CPU_WORKERS + IO_WORKERS;

// Monitor sampling interval
const MONITOR_INTERVAL_MS: u64 = 50;

// ────────────────────────────────────────────────────────────────────────────
// Run one experiment
// ────────────────────────────────────────────────────────────────────────────

fn run_experiment(label: &str, cfg: WorkloadConfig) {
    println!();
    println!("┌──────────────────────────────────────────────────────┐");
    println!("│  EXPERIMENT: {:<40}│", label);
    println!("│  CPU workers: {}   IO workers: {}   Total: {}        │",
        CPU_WORKERS, IO_WORKERS, TOTAL_WORKERS);
    println!("│  Tasks: {}   Seed: {}                              │",
        cfg.num_tasks, SEED);
    println!("└──────────────────────────────────────────────────────┘");

    // 1. Generate tasks (reproducible via fixed seed)
    let mut rng = StdRng::seed_from_u64(SEED);
    let tasks = task::generate_tasks(&cfg, &mut rng);
    println!("  Generated {} tasks ({} CPU, {} IO)",
        tasks.len(),
        tasks.iter().filter(|t| t.kind == task::TaskKind::Cpu).count(),
        tasks.iter().filter(|t| t.kind == task::TaskKind::Io).count(),
    );

    // 2. Shared infrastructure
    let queues = queue::QueuePair::new();
    let shared_metrics = metrics::new_shared(TOTAL_WORKERS);

    // 3. Spawn worker pool
    let (worker_handles, completion_rx, _active_workers) = worker::spawn_pool(
        CPU_WORKERS,
        IO_WORKERS,
        queues.cpu.clone(),
        queues.io.clone(),
    );

    // 4. Spawn metrics collector (drains completion channel → metrics store)
    let collector_handle =
        dispatcher::spawn_collector(completion_rx, shared_metrics.clone());

    // 5. Spawn monitor (queue depth sampling)
    let (monitor_stop_tx, monitor_stop_rx) = mpsc::channel::<()>();
    let monitor_handle = dispatcher::spawn_monitor(
        queues.clone(),
        shared_metrics.clone(),
        MONITOR_INTERVAL_MS,
        monitor_stop_rx,
    );

    // 6. Spawn generator (simulates task arrivals over time)
    let (gen_done_tx, gen_done_rx) = mpsc::sync_channel::<()>(1);
    let gen_handle = dispatcher::spawn_generator(tasks, queues.clone(), gen_done_tx);

    // 7. Wait for the generator to finish dispatching all tasks
    gen_done_rx.recv().expect("generator thread panicked");
    println!("  All tasks dispatched — waiting for workers to finish…");

    // 8. Close queues — signals workers to exit once drained
    queues.close_all();

    // 9. Wait for all worker threads to exit
    for h in worker_handles {
        h.join().expect("worker thread panicked");
    }

    // 10. Stop the monitor (workers are done, no more samples needed)
    let _ = monitor_stop_tx.send(());
    monitor_handle.join().expect("monitor thread panicked");

    // 11. Collector will exit automatically when all worker senders drop
    collector_handle.join().expect("collector thread panicked");

    // 12. Join the generator (already finished)
    gen_handle.join().expect("generator thread panicked");

    // 13. Print the final report
    {
        let m = shared_metrics.lock().unwrap();
        m.print_report(label);
    }
}

// ────────────────────────────────────────────────────────────────────────────
// Entry point
// ────────────────────────────────────────────────────────────────────────────

fn main() {
    println!("╔══════════════════════════════════════════════════════╗");
    println!("║     CONCURRENT TASK DISPATCHER — Rust Demo           ║");
    println!("║     Policy: Priority + Class-Reserved Worker Pool    ║");
    println!("╚══════════════════════════════════════════════════════╝");

    // Experiment A: balanced workload
    run_experiment("Experiment A — Balanced Workload", WorkloadConfig::balanced());

    // Brief pause between experiments for clarity
    thread::sleep(Duration::from_millis(200));

    // Experiment B: stressed workload (CPU-heavy, burst arrivals)
    run_experiment("Experiment B — Stressed Workload", WorkloadConfig::stressed());

    println!();
    println!("All experiments complete. Goodbye.");
}