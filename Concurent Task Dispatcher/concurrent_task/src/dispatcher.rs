// dispatcher.rs — Task generator and dispatcher threads
//
// The generator thread simulates real-time task arrival:
//   • It holds a sorted list of tasks (sorted by arrival_time).
//   • It sleeps until the next task is "due", then pushes it to the
//     appropriate queue via QueuePair.
//
// The dispatcher role is absorbed into the generator in this design because
// the two-queue architecture already separates CPU and IO workloads. The
// generator simply routes each task to the correct typed queue. This is
// Option 3 from the handout (two-queue system) combined with Option 1
// (central dispatcher) at minimal complexity cost.
//
// A separate monitor thread periodically samples queue depths and records
// them in the metrics store.

use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use crate::metrics::SharedMetrics;
use crate::queue::QueuePair;
use crate::task::Task;

/// Spawn the generator thread.
///
/// `tasks` must be sorted by `arrival_time` (ascending).
/// The generator sleeps until each task's arrival time, then pushes it.
/// When all tasks have been dispatched it sends a signal on `done_tx`.
pub fn spawn_generator(
    mut tasks: Vec<Task>,
    queues: QueuePair,
    done_tx: mpsc::SyncSender<()>,
) -> thread::JoinHandle<()> {
    // Sort by arrival time (tasks may already be sorted, but be safe).
    tasks.sort_by_key(|t| t.arrival_time);

    thread::spawn(move || {
        let run_start = Instant::now();

        for task in tasks {
            // How long until this task is "due"?
            let due_at = task.arrival_time;
            let now = Instant::now();
            if due_at > now {
                thread::sleep(due_at.duration_since(now));
            }

            // Route to the appropriate queue
            queues.push(task);
        }

        // All tasks dispatched
        let _ = done_tx.send(());
        let _ = run_start; // suppress unused-variable warning
    })
}

/// Spawn the monitor thread.
///
/// Every `interval` milliseconds, samples both queue lengths and stores
/// them in the shared metrics. Runs until `stop_rx` receives a value.
pub fn spawn_monitor(
    queues: QueuePair,
    metrics: SharedMetrics,
    interval_ms: u64,
    stop_rx: mpsc::Receiver<()>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let interval = Duration::from_millis(interval_ms);
        loop {
            // Check for stop signal (non-blocking)
            match stop_rx.try_recv() {
                Ok(_) | Err(mpsc::TryRecvError::Disconnected) => break,
                Err(mpsc::TryRecvError::Empty) => {}
            }

            let (cpu_len, io_len) = queues.lengths();
            {
                let mut m = metrics.lock().unwrap();
                m.record_queue_sample(cpu_len, io_len);
            }

            thread::sleep(interval);
        }
    })
}

/// Spawn the metrics collector thread.
///
/// Drains the `CompletedTask` channel and records each entry.
/// Returns when the channel is disconnected (i.e., all workers have exited).
pub fn spawn_collector(
    rx: mpsc::Receiver<crate::metrics::CompletedTask>,
    metrics: SharedMetrics,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        for ct in rx {
            let mut m = metrics.lock().unwrap();
            m.record_completion(ct);
        }
    })
}