// worker.rs — Bounded worker pool
//
// Architecture decision:
//   Workers are split into two roles:
//     • CPU workers  (indices 0 .. CPU_WORKERS-1) — pull exclusively from the
//       CPU queue.
//     • IO  workers  (indices CPU_WORKERS .. TOTAL-1) — pull exclusively from
//       the IO queue.
//
//   This reservation prevents CPU-heavy workloads from starving IO tasks and
//   vice versa. It is the "reserve some workers for CPU tasks" policy.
//
//   Each worker thread loops: pop a task → stamp start_time → sleep to
//   simulate work → stamp completion_time → send CompletedTask to the
//   metrics channel.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Instant;

use crate::metrics::CompletedTask;
use crate::queue::TaskQueue;

/// Spawn one worker thread that pulls from `queue`.
///
/// * `worker_id`   — identifies the worker in reports
/// * `queue`       — the TaskQueue this worker is dedicated to
/// * `tx`          — channel to send completed-task records to the collector
/// * `active_workers` — shared counter decremented when the worker exits
pub fn spawn_worker(
    worker_id: usize,
    queue: TaskQueue,
    tx: mpsc::Sender<CompletedTask>,
    active_workers: Arc<AtomicUsize>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        loop {
            // Block until a task is available or the queue is closed.
            let mut task = match queue.pop_or_closed() {
                Some(t) => t,
                None => break, // queue is drained and closed → shut down
            };

            // Mark when execution begins (determines wait time)
            task.start_time = Some(Instant::now());
            task.worker_id = Some(worker_id);

            // Simulate the work: CPU-bound tasks spin; IO-bound tasks sleep.
            // Both are modelled here with thread::sleep for simplicity, but
            // the duration distribution already reflects the cost difference.
            thread::sleep(task.duration);

            // Mark completion
            task.completion_time = Some(Instant::now());

            // Send the completed record to the metrics collector.
            // If the collector is gone (e.g., due to a bug), we just move on.
            if let Some(ct) = CompletedTask::from_task(&task) {
                let _ = tx.send(ct);
            }
        }

        // Decrement the shared counter so the dispatcher knows when all
        // workers have exited.
        active_workers.fetch_sub(1, Ordering::SeqCst);
    })
}

/// Spawn the full worker pool and return:
///   * all JoinHandles
///   * the receiver end of the completion channel
///   * the shared active-worker counter (so the caller can poll for 0)
pub fn spawn_pool(
    num_cpu_workers: usize,
    num_io_workers: usize,
    cpu_queue: TaskQueue,
    io_queue: TaskQueue,
) -> (
    Vec<thread::JoinHandle<()>>,
    mpsc::Receiver<CompletedTask>,
    Arc<AtomicUsize>,
) {
    let total = num_cpu_workers + num_io_workers;
    let (tx, rx) = mpsc::channel::<CompletedTask>();
    let active = Arc::new(AtomicUsize::new(total));
    let mut handles = Vec::with_capacity(total);

    for id in 0..num_cpu_workers {
        handles.push(spawn_worker(
            id,
            cpu_queue.clone(),
            tx.clone(),
            Arc::clone(&active),
        ));
    }

    for id in 0..num_io_workers {
        handles.push(spawn_worker(
            num_cpu_workers + id,
            io_queue.clone(),
            tx.clone(),
            Arc::clone(&active),
        ));
    }

    (handles, rx, active)
}