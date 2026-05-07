// task.rs — Task model and workload generation

use std::time::{Duration, Instant};
use rand::rngs::StdRng;
use rand::Rng;

/// Whether a task is CPU-bound or I/O-bound.
/// This distinction drives scheduling: CPU tasks are assigned to
/// dedicated CPU workers; IO tasks go to IO workers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskKind {
    Cpu,
    Io,
}

impl std::fmt::Display for TaskKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskKind::Cpu => write!(f, "CPU"),
            TaskKind::Io => write!(f, "IO "),
        }
    }
}

/// A single unit of work submitted to the scheduler.
#[derive(Debug, Clone)]
pub struct Task {
    /// Unique identifier
    pub id: u64,
    /// Wall-clock instant when the task was "generated" by the workload
    pub arrival_time: Instant,
    /// CPU or IO
    pub kind: TaskKind,
    /// How long simulated execution takes (sleep duration)
    pub duration: Duration,
    /// 1 (low) – 5 (high): used by the priority-aware dispatcher
    pub priority: u8,
    /// Time at which the task was actually picked up by a worker
    pub start_time: Option<Instant>,
    /// Time at which the task finished
    pub completion_time: Option<Instant>,
    /// Which worker thread executed this task
    pub worker_id: Option<usize>,
}

impl Task {
    pub fn new(
        id: u64,
        arrival_time: Instant,
        kind: TaskKind,
        duration: Duration,
        priority: u8,
    ) -> Self {
        Task {
            id,
            arrival_time,
            kind,
            duration,
            priority,
            start_time: None,
            completion_time: None,
            worker_id: None,
        }
    }

    /// Wait time = start – arrival  (time spent in the queue)
    pub fn wait_time(&self) -> Option<Duration> {
        self.start_time.map(|s| s.duration_since(self.arrival_time))
    }

    /// Turnaround = completion – arrival
    pub fn turnaround_time(&self) -> Option<Duration> {
        self.completion_time
            .map(|c| c.duration_since(self.arrival_time))
    }
}

// ────────────────────────────────────────────────────────────────────────────
// Workload generation
// ────────────────────────────────────────────────────────────────────────────

/// Parameters controlling how tasks are generated.
pub struct WorkloadConfig {
    /// How many tasks to generate
    pub num_tasks: usize,
    /// Probability [0,1] that a task is CPU-bound
    pub cpu_fraction: f64,
    /// Minimum task duration in milliseconds
    pub min_duration_ms: u64,
    /// Maximum task duration in milliseconds
    pub max_duration_ms: u64,
    /// Maximum inter-arrival gap in milliseconds (0 = simultaneous burst)
    pub max_arrival_gap_ms: u64,
    /// Whether to add a large burst of tasks at the start (stress mode)
    pub burst_at_start: bool,
}

impl WorkloadConfig {
    /// Experiment A: balanced workload, steady arrivals
    pub fn balanced() -> Self {
        WorkloadConfig {
            num_tasks: 600,
            cpu_fraction: 0.50,
            min_duration_ms: 5,
            max_duration_ms: 80,
            max_arrival_gap_ms: 8,
            burst_at_start: false,
        }
    }

    /// Experiment B: stressed workload — CPU-heavy, burst arrivals, uneven durations
    pub fn stressed() -> Self {
        WorkloadConfig {
            num_tasks: 700,
            cpu_fraction: 0.85,
            min_duration_ms: 2,
            max_duration_ms: 150,
            max_arrival_gap_ms: 2,
            burst_at_start: true,
        }
    }
}

/// Generate a reproducible list of tasks using a seeded RNG.
pub fn generate_tasks(cfg: &WorkloadConfig, rng: &mut StdRng) -> Vec<Task> {
    let mut tasks = Vec::with_capacity(cfg.num_tasks);
    let mut elapsed_ms: u64 = 0;

    for id in 0..cfg.num_tasks as u64 {
        // Arrival time: accumulate gaps
        let gap = if cfg.burst_at_start && id < 50 {
            0 // first 50 tasks arrive simultaneously in stressed mode
        } else {
            rng.gen_range(0..=cfg.max_arrival_gap_ms)
        };
        elapsed_ms += gap;
        let arrival_time = Instant::now() + Duration::from_millis(elapsed_ms);

        let kind = if rng.gen_range(0.0f64..1.0) < cfg.cpu_fraction {
            TaskKind::Cpu
        } else {
            TaskKind::Io
        };

        let duration_ms = rng.gen_range(cfg.min_duration_ms..=cfg.max_duration_ms);
        let priority = rng.gen_range(1u8..=5u8);

        tasks.push(Task::new(
            id,
            arrival_time,
            kind,
            Duration::from_millis(duration_ms),
            priority,
        ));
    }

    tasks
}