use crate::await_tree::AWAIT_TREE_REGISTRY;
use crate::metric::PANIC_SIGNAL;
use backtrace::Backtrace;
use log::error;
use once_cell::sync::Lazy;
use std::panic;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;

pub static PANIC_TAG: Lazy<AtomicBool> = Lazy::new(|| AtomicBool::new(false));

// Refer from the greptimedb:
// https://github.com/GreptimeTeam/greptimedb/blob/main/src/common/telemetry/src/panic_hook.rs
pub fn set_panic_hook() {
    let default_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic| {
        let backtrace = Backtrace::new();
        let backtrace = format!("{backtrace:?}");
        if let Some(location) = panic.location() {
            error!(
                "[Panic] ============================================================\
                \nmessage: {}\n backtrace: {}\n file: {}. line: {}. column: {}\n\
                ====================================================================",
                panic,
                backtrace,
                location.file(),
                location.line(),
                location.column()
            );
        } else {
            error!(
                "[Panic] ============================================================\
                \nmessage: {}\n backtrace: {}\n\
                ====================================================================",
                panic, backtrace,
            );
        }

        PANIC_TAG.store(true, SeqCst);
        PANIC_SIGNAL.set(1);

        default_hook(panic);
    }));
}

#[cfg(test)]
mod test {
    use crate::panic_hook::{set_panic_hook, PANIC_TAG};
    use std::sync::atomic::Ordering::SeqCst;
    use std::thread::sleep;
    use std::time::Duration;

    #[test]
    fn test_thread_internal_panic() {
        set_panic_hook();

        std::thread::spawn(|| {
            sleep(Duration::from_millis(100));
            panic!();
        });

        awaitility::at_most(Duration::from_secs(1)).until(|| PANIC_TAG.load(SeqCst));
    }
}
