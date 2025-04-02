use crate::metric::DEADLOCK_SIGNAL;
use log::{error, info, warn};
use once_cell::sync::Lazy;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;
use std::time::Duration;

pub static DEADLOCK_TAG: Lazy<AtomicBool> = Lazy::new(|| AtomicBool::new(false));

pub fn detect_deadlock() {
    let _ = std::thread::spawn(move || loop {
        std::thread::sleep(Duration::from_secs(2));
        let deadlocks = parking_lot::deadlock::check_deadlock();
        if deadlocks.is_empty() {
            continue;
        }

        error!("{} deadlocks detected", deadlocks.len());
        for (i, threads) in deadlocks.iter().enumerate() {
            error!("Deadlock #{}", i);
            for t in threads {
                error!("Thread Id {:#?}", t.thread_id());
                error!("{:#?}", t.backtrace());
            }
        }
        DEADLOCK_SIGNAL.set(1);
        DEADLOCK_TAG.store(true, SeqCst);
    });
}

#[cfg(test)]
mod tests {
    use crate::deadlock::{detect_deadlock, DEADLOCK_TAG};
    use parking_lot::Mutex;
    use std::sync::atomic::Ordering;
    use std::sync::atomic::Ordering::SeqCst;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_detect_deadlock() {
        let mutex1 = Arc::new(Mutex::new(0));
        let mutex2 = Arc::new(Mutex::new(0));

        let m1_clone = Arc::clone(&mutex1);
        let m2_clone = Arc::clone(&mutex2);

        let handle1 = thread::spawn(move || {
            let _lock1 = mutex1.lock();
            thread::sleep(Duration::from_millis(100)); // Sleep to ensure the deadlock
            let _lock2 = mutex2.lock();
        });

        let handle2 = thread::spawn(move || {
            let _lock2 = m2_clone.lock();
            thread::sleep(Duration::from_millis(100)); // Sleep to ensure the deadlock
            let _lock1 = m1_clone.lock();
        });

        // Start the deadlock detection thread
        detect_deadlock();

        awaitility::at_most(Duration::from_secs(5)).until(|| DEADLOCK_TAG.load(SeqCst));
    }
}
