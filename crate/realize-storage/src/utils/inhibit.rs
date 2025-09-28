//! A sync utility that creates barrier whose access can be inhibited.
///
/// To use this type, create a new [Inhibit] instance and obtain a
/// [Barrier] from it, then call [Barrier::allow] at the point in the
/// code that should be inhibited.
///
/// From other places in the code, call [Inhibit::inhibit] and hold on
/// to the guard for as long as the barrier should be prevented from
/// allowing execution. It's possible to inhibit execution more than
/// once, from multiple points and multiple cloned [Inhibit] instance;
/// the [Barrier] will only allow through once all inhibiting guards
/// have been dropped.
use tokio::sync::watch;

/// A [Inhibit] instance that creates and bridges [Barrier]s and
/// [Guard]s.
#[derive(Clone)]
pub(crate) struct Inhibit {
    tx: watch::Sender<u32>,
}

impl Inhibit {
    /// Create a new [Inhibit] instance, which by default allows barriers.
    pub(crate) fn new() -> Self {
        let (tx, _) = watch::channel(0);
        Self { tx }
    }

    /// Disallow a barrier as long as the returned guard is held on to.
    pub(crate) fn inhibit(&self) -> Guard {
        self.tx.send_modify(|n| *n += 1);
        Guard {
            tx: self.tx.clone(),
        }
    }

    /// Create a new barrier.
    pub(crate) fn barrier(&self) -> Barrier {
        Barrier {
            rx: self.tx.subscribe(),
        }
    }
}

/// A type whose passage can be inhibited by linked [Inhibit]
/// instances.
pub(crate) struct Barrier {
    rx: watch::Receiver<u32>,
}

impl Barrier {
    /// Wait until nothing inhibit passage, that is, there is no
    /// [Inhibit::Guard] instance elsewhere in the code.
    pub(crate) async fn allow(&mut self) {
        let _ = self.rx.wait_for(|n| *n == 0).await;
    }

    /// Check the number of [Inhibit::Guard] in effect.
    #[cfg(test)]
    fn inhibition_count(&self) -> u32 {
        *self.rx.borrow()
    }
}

/// A [Guard] prevents disallows any associated [Barrier] until
/// dropped.
pub(crate) struct Guard {
    tx: watch::Sender<u32>,
}

impl Drop for Guard {
    fn drop(&mut self) {
        self.tx.send_modify(|n| {
            assert!(*n > 0);
            *n -= 1;
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[tokio::test]
    async fn basic_inhibit_creation() {
        let inhibit = Inhibit::new();
        let barrier = inhibit.barrier();

        // Initially no inhibitions
        assert_eq!(barrier.inhibition_count(), 0);

        // Should allow immediately
        let mut barrier = inhibit.barrier();
        barrier.allow().await;
    }

    #[tokio::test]
    async fn barrier_and_guard_creation() {
        let inhibit = Inhibit::new();
        let barrier1 = inhibit.barrier();
        let barrier2 = inhibit.barrier();

        let guard1 = inhibit.inhibit();
        assert_eq!(barrier1.inhibition_count(), 1);
        assert_eq!(barrier2.inhibition_count(), 1);

        let guard2 = inhibit.inhibit();
        assert_eq!(barrier1.inhibition_count(), 2);
        assert_eq!(barrier2.inhibition_count(), 2);

        drop(guard1);
        assert_eq!(barrier1.inhibition_count(), 1);
        assert_eq!(barrier2.inhibition_count(), 1);

        drop(guard2);
        assert_eq!(barrier1.inhibition_count(), 0);
        assert_eq!(barrier2.inhibition_count(), 0);
    }

    #[tokio::test]
    async fn single_guard_inhibits_barrier() {
        let inhibit = Inhibit::new();
        let mut barrier = inhibit.barrier();

        let ready = Arc::new(tokio::sync::Barrier::new(2));
        let guard_created = Arc::new(tokio::sync::Barrier::new(2));
        let dropped = Arc::new(std::sync::Mutex::new(false));
        let inhibit_clone = inhibit.clone();
        tokio::spawn({
            let dropped = dropped.clone();
            let ready = ready.clone();
            let guard_created = guard_created.clone();
            async move {
                ready.wait().await;
                let _guard = inhibit_clone.inhibit();
                guard_created.wait().await;
                *(dropped.lock().unwrap()) = true;
            }
        });

        ready.wait().await;
        guard_created.wait().await;
        assert_eq!(barrier.inhibition_count(), 1);

        barrier.allow().await;

        assert!(*dropped.lock().unwrap());
        assert_eq!(barrier.inhibition_count(), 0);
    }

    #[tokio::test]
    async fn multiple_guards_same_inhibit() {
        let inhibit = Inhibit::new();
        let mut barrier = inhibit.barrier();

        // Synchronization barriers to coordinate guard creation and dropping
        let ready = Arc::new(tokio::sync::Barrier::new(3)); // main + 2 tasks
        let first_dropped = Arc::new(tokio::sync::Barrier::new(2)); // main + first task
        let second_dropped = Arc::new(tokio::sync::Barrier::new(2)); // main + second task

        let guard_count = Arc::new(Mutex::new(0));

        // First guard task
        let inhibit_clone1 = inhibit.clone();
        let ready1 = ready.clone();
        let first_dropped1 = first_dropped.clone();
        let guard_count1 = guard_count.clone();
        tokio::spawn(async move {
            let _guard1 = inhibit_clone1.inhibit();
            *guard_count1.lock().unwrap() += 1;
            ready1.wait().await;
            // Wait until we're told to drop
            first_dropped1.wait().await;
            // Guard drops here
        });

        // Second guard task
        let inhibit_clone2 = inhibit.clone();
        let ready2 = ready.clone();
        let second_dropped2 = second_dropped.clone();
        let guard_count2 = guard_count.clone();
        tokio::spawn(async move {
            let _guard2 = inhibit_clone2.inhibit();
            *guard_count2.lock().unwrap() += 1;
            ready2.wait().await;
            // Wait until we're told to drop
            second_dropped2.wait().await;
            // Guard drops here
        });

        // Wait for both guards to be created
        ready.wait().await;
        assert_eq!(*guard_count.lock().unwrap(), 2);
        assert_eq!(barrier.inhibition_count(), 2);

        // Start the barrier waiting in a task
        let barrier_done = Arc::new(Mutex::new(false));
        let barrier_done_clone = barrier_done.clone();
        let barrier_task = tokio::spawn(async move {
            barrier.allow().await;
            *barrier_done_clone.lock().unwrap() = true;
        });

        // Verify barrier is still waiting
        tokio::task::yield_now().await;
        assert!(!*barrier_done.lock().unwrap());

        // Drop first guard
        first_dropped.wait().await;
        tokio::task::yield_now().await;
        assert!(!*barrier_done.lock().unwrap()); // Still waiting for second guard

        // Drop second guard
        second_dropped.wait().await;

        // Now barrier should complete
        barrier_task.await.unwrap();
        assert!(*barrier_done.lock().unwrap());
    }

    #[tokio::test]
    async fn multiple_guards_cloned_inhibit() {
        let inhibit = Arc::new(Inhibit::new());
        let mut barrier = inhibit.barrier();

        let ready = Arc::new(tokio::sync::Barrier::new(3)); // main + 2 tasks
        let drop_guards = Arc::new(tokio::sync::Barrier::new(3)); // main + 2 tasks

        let inhibit_clone1 = Arc::clone(&inhibit);
        let ready1 = ready.clone();
        let drop_guards1 = drop_guards.clone();
        tokio::spawn(async move {
            let _guard1 = inhibit_clone1.inhibit();
            ready1.wait().await;
            drop_guards1.wait().await;
            // Guard drops here
        });

        let inhibit_clone2 = Arc::clone(&inhibit);
        let ready2 = ready.clone();
        let drop_guards2 = drop_guards.clone();
        tokio::spawn(async move {
            let _guard2 = inhibit_clone2.inhibit();
            ready2.wait().await;
            drop_guards2.wait().await;
            // Guard drops here
        });

        // Wait for both guards to be created
        ready.wait().await;
        assert_eq!(barrier.inhibition_count(), 2);

        // Start barrier waiting in a task
        let barrier_done = Arc::new(Mutex::new(false));
        let barrier_done_clone = barrier_done.clone();
        let barrier_task = tokio::spawn(async move {
            barrier.allow().await;
            *barrier_done_clone.lock().unwrap() = true;
        });

        // Verify barrier is still waiting
        tokio::task::yield_now().await;
        assert!(!*barrier_done.lock().unwrap());

        // Release both guards
        drop_guards.wait().await;

        // Barrier should now complete
        barrier_task.await.unwrap();
        assert!(*barrier_done.lock().unwrap());
    }

    #[tokio::test]
    async fn guard_auto_cleanup() {
        let inhibit = Inhibit::new();
        let mut barrier = inhibit.barrier();

        {
            let _guard = inhibit.inhibit();
            assert_eq!(barrier.inhibition_count(), 1);
        } // Guard dropped here

        assert_eq!(barrier.inhibition_count(), 0);

        // Should allow immediately now
        barrier.allow().await;
    }

    #[tokio::test]
    async fn barrier_allows_immediately_no_guards() {
        let inhibit = Inhibit::new();
        let mut barrier = inhibit.barrier();

        assert_eq!(barrier.inhibition_count(), 0);

        // Should complete immediately
        barrier.allow().await;
    }

    #[tokio::test]
    async fn barrier_awaits_many_concurrent_guards() {
        let inhibit = Inhibit::new();

        let ready = Arc::new(tokio::sync::Barrier::new(11)); // main + 10 tasks
        let drop_guards = Arc::new(tokio::sync::Barrier::new(11)); // main + 10 tasks

        // Create 10 guards
        let mut handles = Vec::new();
        for _ in 0..10 {
            let inhibit_clone = inhibit.clone();
            let ready_clone = ready.clone();
            let drop_guards_clone = drop_guards.clone();
            handles.push(tokio::spawn(async move {
                let _guard = inhibit_clone.inhibit();
                ready_clone.wait().await;
                drop_guards_clone.wait().await;
                // Guard drops here
            }));
        }

        // Wait for all guards to be created
        ready.wait().await;

        let mut barrier = inhibit.barrier();
        assert_eq!(barrier.inhibition_count(), 10);

        // Start barrier waiting in a task
        let barrier_done = Arc::new(Mutex::new(false));
        let barrier_done_clone = barrier_done.clone();
        let barrier_task = tokio::spawn(async move {
            barrier.allow().await;
            *barrier_done_clone.lock().unwrap() = true;
        });

        // Verify barrier is waiting
        tokio::task::yield_now().await;
        assert!(!*barrier_done.lock().unwrap());

        // Release all guards
        drop_guards.wait().await;

        // Wait for all guard tasks to complete
        for handle in handles {
            handle.await.unwrap();
        }

        // Barrier should complete
        barrier_task.await.unwrap();
        assert!(*barrier_done.lock().unwrap());
    }

    #[tokio::test]
    async fn multiple_independent_barriers() {
        let inhibit = Inhibit::new();

        let ready = Arc::new(tokio::sync::Barrier::new(2)); // main + guard task
        let drop_guard = Arc::new(tokio::sync::Barrier::new(2)); // main + guard task

        // Create and hold a guard
        let inhibit_clone = inhibit.clone();
        let ready_clone = ready.clone();
        let drop_guard_clone = drop_guard.clone();
        let guard_task = tokio::spawn(async move {
            let _guard = inhibit_clone.inhibit();
            ready_clone.wait().await;
            drop_guard_clone.wait().await;
            // Guard drops here
        });

        // Wait for guard to be created
        ready.wait().await;

        // Create both barriers
        let mut barrier1 = inhibit.barrier();
        let mut barrier2 = inhibit.barrier();
        assert_eq!(barrier1.inhibition_count(), 1);
        assert_eq!(barrier2.inhibition_count(), 1);

        // Start both barriers waiting
        let barrier1_done = Arc::new(Mutex::new(false));
        let barrier1_done_clone = barrier1_done.clone();
        let barrier1_task = tokio::spawn(async move {
            barrier1.allow().await;
            *barrier1_done_clone.lock().unwrap() = true;
        });

        let barrier2_done = Arc::new(Mutex::new(false));
        let barrier2_done_clone = barrier2_done.clone();
        let barrier2_task = tokio::spawn(async move {
            barrier2.allow().await;
            *barrier2_done_clone.lock().unwrap() = true;
        });

        // Verify both barriers are waiting
        tokio::task::yield_now().await;
        assert!(!*barrier1_done.lock().unwrap());
        assert!(!*barrier2_done.lock().unwrap());

        // Release the guard
        drop_guard.wait().await;

        // Wait for all to complete
        guard_task.await.unwrap();
        barrier1_task.await.unwrap();
        barrier2_task.await.unwrap();

        // Both barriers should have completed
        assert!(*barrier1_done.lock().unwrap());
        assert!(*barrier2_done.lock().unwrap());
    }

    #[tokio::test]
    async fn inhibition_count_correctness() {
        let inhibit = Inhibit::new();
        let barrier = inhibit.barrier();
        let inhibit_clone = inhibit.clone();

        // 0 -> 3
        let guard1 = inhibit.inhibit();
        let guard2 = inhibit_clone.inhibit();
        let guard3 = inhibit.inhibit();
        assert_eq!(barrier.inhibition_count(), 3);

        // 3 -> 1
        drop(guard1);
        drop(guard2);
        assert_eq!(barrier.inhibition_count(), 1);

        // 1 -> 0
        drop(guard3);
        assert_eq!(barrier.inhibition_count(), 0);

        // Create new barrier from clone to ensure it also sees 0
        let barrier_clone = inhibit_clone.barrier();
        assert_eq!(barrier_clone.inhibition_count(), 0);
    }

    #[tokio::test]
    async fn concurrent_stress_test() {
        let inhibit = Arc::new(Inhibit::new());
        let barrier = inhibit.barrier();

        let operation_counter = Arc::new(Mutex::new(0u32));
        let total_operations = 250; // 50 tasks × 5 operations each
        let ready = Arc::new(tokio::sync::Barrier::new(51)); // main + 50 tasks
        let mut tasks = Vec::new();

        // Create 50 tasks that create/drop guards and occasionally wait on barriers
        for i in 0..50 {
            let inhibit_clone = Arc::clone(&inhibit);
            let counter_clone = Arc::clone(&operation_counter);
            let ready_clone = ready.clone();
            tasks.push(tokio::spawn(async move {
                for j in 0..5 {
                    if (i + j) % 7 == 0 {
                        // Occasionally create a barrier and try to pass through
                        let mut barrier = inhibit_clone.barrier();
                        if barrier.inhibition_count() == 0 {
                            // Only try to allow if no guards are present
                            barrier.allow().await;
                        }
                    } else {
                        // Create guard and hold briefly
                        let _guard = inhibit_clone.inhibit();
                        // Just yield to allow other tasks to run
                        tokio::task::yield_now().await;
                    }

                    // Count completed operations
                    *counter_clone.lock().unwrap() += 1;
                }
                ready_clone.wait().await;
            }));
        }

        // Wait for all tasks to complete
        ready.wait().await;

        for task in tasks {
            task.await.unwrap();
        }

        // Verify all operations completed
        assert_eq!(*operation_counter.lock().unwrap(), total_operations);

        // Final state should have no inhibitions
        assert_eq!(barrier.inhibition_count(), 0);

        // Final barrier should allow immediately
        let mut final_barrier = inhibit.barrier();
        final_barrier.allow().await;
    }
}
