// metrics.rs — Instrumentation and statistics collection
//
// CompletedTask records arrive on a channel from worker threads.
// The MetricsCollector aggregates them and prints a final report.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::task::{Task, TaskKind};

/// A snapshot of one completed task sent from a worker to the collector.
pub struct CompletedTask {
    pub id: u64,
    pub kind: TaskKind,
    pub worker_id: usize,
    pub wait_time: Duration,
    pub turnaround_time: Duration,
    pub duration: Duration,
}

impl CompletedTask {
    pub fn from_task(t: &Task) -> Option<Self> {
        let wait = t.wait_time()?;
        let turnaround = t.turnaround_time()?;
        Some(CompletedTask {
            id: t.id,
            kind: t.kind.clone(),
            worker_id: t.worker_id?,
            wait_time: wait,
            turnaround_time: turnaround,
            duration: t.duration,
        })
    }
}

// ────────────────────────────────────────────────────────────────────────────
// Per-worker stats (for utilization)
// ────────────────────────────────────────────────────────────────────────────

#[derive(Default)]
pub struct WorkerStats {
    pub tasks_done: u64,
    pub busy_time: Duration,
}

// ────────────────────────────────────────────────────────────────────────────
// Central metrics store — shared via Arc<Mutex<_>>
// ────────────────────────────────────────────────────────────────────────────

pub struct Metrics {
    pub start: Instant,

    // Completion records
    pub completed: Vec<CompletedTask>,

    // Aggregate totals (updated incrementally to avoid re-scanning)
    pub total_wait: Duration,
    pub total_turnaround: Duration,
    pub max_wait: Duration,
    pub cpu_done: u64,
    pub io_done: u64,

    // Queue length samples taken periodically
    pub queue_samples: Vec<(usize, usize)>, // (cpu_len, io_len)

    // Per-worker busy time
    pub worker_stats: Vec<WorkerStats>,

    // How many workers exist (set at init)
    pub num_workers: usize,
}

impl Metrics {
    pub fn new(num_workers: usize) -> Self {
        let mut worker_stats = Vec::with_capacity(num_workers);
        for _ in 0..num_workers {
            worker_stats.push(WorkerStats::default());
        }
        Metrics {
            start: Instant::now(),
            completed: Vec::new(),
            total_wait: Duration::ZERO,
            total_turnaround: Duration::ZERO,
            max_wait: Duration::ZERO,
            cpu_done: 0,
            io_done: 0,
            queue_samples: Vec::new(),
            worker_stats,
            num_workers,
        }
    }

    pub fn record_completion(&mut self, ct: CompletedTask) {
        self.total_wait += ct.wait_time;
        self.total_turnaround += ct.turnaround_time;
        if ct.wait_time > self.max_wait {
            self.max_wait = ct.wait_time;
        }
        match ct.kind {
            TaskKind::Cpu => self.cpu_done += 1,
            TaskKind::Io => self.io_done += 1,
        }
        if ct.worker_id < self.num_workers {
            self.worker_stats[ct.worker_id].tasks_done += 1;
            self.worker_stats[ct.worker_id].busy_time += ct.duration;
        }
        self.completed.push(ct);
    }

    pub fn record_queue_sample(&mut self, cpu_len: usize, io_len: usize) {
        self.queue_samples.push((cpu_len, io_len));
    }

    // ────────────────────────────────────────────────────────────────────
    // Report
    // ────────────────────────────────────────────────────────────────────

    pub fn print_report(&self, label: &str) {
        let n = self.completed.len() as u64;
        let makespan = self.start.elapsed();

        let avg_wait = if n > 0 {
            self.total_wait / n as u32
        } else {
            Duration::ZERO
        };
        let avg_turnaround = if n > 0 {
            self.total_turnaround / n as u32
        } else {
            Duration::ZERO
        };

        // Throughput
        let throughput = if makespan.as_secs_f64() > 0.0 {
            n as f64 / makespan.as_secs_f64()
        } else {
            0.0
        };

        // Queue length stats
        let (avg_cpu_q, avg_io_q, max_cpu_q, max_io_q) = self.queue_length_stats();

        // Worker utilization
        let total_wall = makespan.as_secs_f64() * self.num_workers as f64;
        let total_busy: f64 = self
            .worker_stats
            .iter()
            .map(|w| w.busy_time.as_secs_f64())
            .sum();
        let utilization = if total_wall > 0.0 {
            total_busy / total_wall * 100.0
        } else {
            0.0
        };

        // Fairness gap: difference in avg wait between CPU and IO tasks
        let (cpu_wait, io_wait) = self.class_avg_wait();

        println!();
        println!("══════════════════════════════════════════════════════");
        println!("  METRICS REPORT — {}", label);
        println!("══════════════════════════════════════════════════════");
        println!("  Total tasks completed   : {}", n);
        println!("    CPU tasks             : {}", self.cpu_done);
        println!("    IO  tasks             : {}", self.io_done);
        println!("  Makespan                : {:.3}s", makespan.as_secs_f64());
        println!("  Throughput              : {:.1} tasks/s", throughput);
        println!("  Avg wait time           : {:.2}ms", avg_wait.as_secs_f64() * 1000.0);
        println!("  Max wait time           : {:.2}ms", self.max_wait.as_secs_f64() * 1000.0);
        println!("  Avg turnaround time     : {:.2}ms", avg_turnaround.as_secs_f64() * 1000.0);
        println!("  Pool utilization        : {:.1}%", utilization);
        println!("  Avg CPU queue depth     : {:.1}", avg_cpu_q);
        println!("  Avg IO  queue depth     : {:.1}", avg_io_q);
        println!("  Max CPU queue depth     : {}", max_cpu_q);
        println!("  Max IO  queue depth     : {}", max_io_q);
        println!("  CPU-class avg wait      : {:.2}ms", cpu_wait * 1000.0);
        println!("  IO-class  avg wait      : {:.2}ms", io_wait * 1000.0);
        println!(
            "  Fairness gap (|Δ wait|) : {:.2}ms",
            (cpu_wait - io_wait).abs() * 1000.0
        );
        println!();
        println!("  Per-worker breakdown:");
        for (i, ws) in self.worker_stats.iter().enumerate() {
            println!(
                "    Worker {:2}: {:4} tasks, busy {:.2}s",
                i,
                ws.tasks_done,
                ws.busy_time.as_secs_f64()
            );
        }
        println!("══════════════════════════════════════════════════════");
    }

    fn queue_length_stats(&self) -> (f64, f64, usize, usize) {
        if self.queue_samples.is_empty() {
            return (0.0, 0.0, 0, 0);
        }
        let n = self.queue_samples.len() as f64;
        let sum_cpu: usize = self.queue_samples.iter().map(|(c, _)| c).sum();
        let sum_io: usize = self.queue_samples.iter().map(|(_, i)| i).sum();
        let max_cpu = self.queue_samples.iter().map(|(c, _)| *c).max().unwrap_or(0);
        let max_io = self.queue_samples.iter().map(|(_, i)| *i).max().unwrap_or(0);
        (sum_cpu as f64 / n, sum_io as f64 / n, max_cpu, max_io)
    }

    fn class_avg_wait(&self) -> (f64, f64) {
        let (mut cpu_sum, mut cpu_n) = (0.0f64, 0u64);
        let (mut io_sum, mut io_n) = (0.0f64, 0u64);
        for ct in &self.completed {
            match ct.kind {
                TaskKind::Cpu => {
                    cpu_sum += ct.wait_time.as_secs_f64();
                    cpu_n += 1;
                }
                TaskKind::Io => {
                    io_sum += ct.wait_time.as_secs_f64();
                    io_n += 1;
                }
            }
        }
        let cpu_avg = if cpu_n > 0 { cpu_sum / cpu_n as f64 } else { 0.0 };
        let io_avg = if io_n > 0 { io_sum / io_n as f64 } else { 0.0 };
        (cpu_avg, io_avg)
    }
}

/// Thread-safe handle to the shared metrics store.
pub type SharedMetrics = Arc<Mutex<Metrics>>;

pub fn new_shared(num_workers: usize) -> SharedMetrics {
    Arc::new(Mutex::new(Metrics::new(num_workers)))
}