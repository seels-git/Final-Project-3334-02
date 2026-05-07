// queue.rs — Two-queue system: separate CPU and IO ready queues
//
// Design rationale:
//   Mixing CPU and IO tasks in one queue leads to unfair head-of-line
//   blocking: a long CPU task blocks IO tasks that could complete quickly.
//   Two separate queues let the dispatcher apply different policies to each
//   task class and reserve workers accordingly.
//
// Each queue is a BinaryHeap ordered by (priority DESC, arrival ASC).
// That gives us priority scheduling with FIFO tie-breaking at the same
// priority level, which prevents starvation of equal-priority tasks.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::{Arc, Condvar, Mutex};

use crate::task::{Task, TaskKind};

// ────────────────────────────────────────────────────────────────────────────
// Priority wrapper so BinaryHeap orders tasks correctly
// ────────────────────────────────────────────────────────────────────────────

struct PrioritizedTask(Task);

impl PartialEq for PrioritizedTask {
    fn eq(&self, other: &Self) -> bool {
        self.0.priority == other.0.priority && self.0.arrival_time == other.0.arrival_time
    }
}
impl Eq for PrioritizedTask {}

impl PartialOrd for PrioritizedTask {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PrioritizedTask {
    fn cmp(&self, other: &Self) -> Ordering {
        // Higher priority number wins; break ties by earlier arrival time
        self.0
            .priority
            .cmp(&other.0.priority)
            .then_with(|| other.0.arrival_time.cmp(&self.0.arrival_time))
    }
}

// ────────────────────────────────────────────────────────────────────────────
// Inner shared state for a single queue
// ────────────────────────────────────────────────────────────────────────────

struct QueueInner {
    heap: BinaryHeap<PrioritizedTask>,
    closed: bool, // set true when no more tasks will ever be pushed
}

// ────────────────────────────────────────────────────────────────────────────
// Public queue handle — cheap to clone (Arc inside)
// ────────────────────────────────────────────────────────────────────────────

/// A concurrent priority queue for tasks of one kind.
///
/// Cloning a `TaskQueue` produces another handle to the *same* underlying
/// queue — this is how the dispatcher and workers share it.
#[derive(Clone)]
pub struct TaskQueue {
    inner: Arc<(Mutex<QueueInner>, Condvar)>,
    kind: TaskKind,
}

impl TaskQueue {
    pub fn new(kind: TaskKind) -> Self {
        TaskQueue {
            inner: Arc::new((
                Mutex::new(QueueInner {
                    heap: BinaryHeap::new(),
                    closed: false,
                }),
                Condvar::new(),
            )),
            kind,
        }
    }

    #[allow(dead_code)]
    pub fn kind(&self) -> &TaskKind {
        &self.kind
    }

    /// Push a task. Wakes one blocked worker.
    pub fn push(&self, task: Task) {
        let (lock, cvar) = &*self.inner;
        let mut guard = lock.lock().unwrap();
        guard.heap.push(PrioritizedTask(task));
        cvar.notify_one();
    }

    /// Try to pop the highest-priority task without blocking.
    /// Returns `None` if the queue is currently empty.
    #[allow(dead_code)]
    pub fn try_pop(&self) -> Option<Task> {
        let (lock, _) = &*self.inner;
        let mut guard = lock.lock().unwrap();
        guard.heap.pop().map(|pt| pt.0)
    }

    /// Block until a task is available or the queue is closed.
    /// Returns `None` when the queue is drained and closed.
    pub fn pop_or_closed(&self) -> Option<Task> {
        let (lock, cvar) = &*self.inner;
        let mut guard = lock.lock().unwrap();
        loop {
            if let Some(pt) = guard.heap.pop() {
                return Some(pt.0);
            }
            if guard.closed {
                return None;
            }
            guard = cvar.wait(guard).unwrap();
        }
    }

    /// Signal that no more tasks will be pushed.
    /// Workers waiting on `pop_or_closed` will be woken and return `None`.
    pub fn close(&self) {
        let (lock, cvar) = &*self.inner;
        let mut guard = lock.lock().unwrap();
        guard.closed = true;
        cvar.notify_all();
    }

    /// Current number of tasks waiting.
    pub fn len(&self) -> usize {
        let (lock, _) = &*self.inner;
        lock.lock().unwrap().heap.len()
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// ────────────────────────────────────────────────────────────────────────────
// Two-queue facade used by the dispatcher and generator
// ────────────────────────────────────────────────────────────────────────────

/// Holds both the CPU queue and the IO queue.
/// Cloning is cheap; all clones share the same underlying queues.
#[derive(Clone)]
pub struct QueuePair {
    pub cpu: TaskQueue,
    pub io: TaskQueue,
}

impl QueuePair {
    pub fn new() -> Self {
        QueuePair {
            cpu: TaskQueue::new(TaskKind::Cpu),
            io: TaskQueue::new(TaskKind::Io),
        }
    }

    /// Route a task to the correct queue by its kind.
    pub fn push(&self, task: Task) {
        match task.kind {
            TaskKind::Cpu => self.cpu.push(task),
            TaskKind::Io => self.io.push(task),
        }
    }

    /// Close both queues so blocked workers can exit.
    pub fn close_all(&self) {
        self.cpu.close();
        self.io.close();
    }

    /// Snapshot of current queue depths (cpu_len, io_len).
    pub fn lengths(&self) -> (usize, usize) {
        (self.cpu.len(), self.io.len())
    }
}