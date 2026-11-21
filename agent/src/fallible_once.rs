use std::sync::OnceLock;

/// Publish only successful initialization. Callers may serialize the larger
/// initialization sequence; a losing concurrent value is dropped normally.
pub(crate) fn get_or_try_init<T>(
    cell: &OnceLock<T>,
    initialize: impl FnOnce() -> Result<T, String>,
) -> Result<&T, String> {
    if let Some(value) = cell.get() {
        return Ok(value);
    }
    let value = initialize()?;
    let _ = cell.set(value);
    cell.get().ok_or_else(|| "initialization was not published".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[test]
    fn failed_allocation_is_an_error_not_a_panic() {
        let cell = OnceLock::<usize>::new();
        let result = std::panic::catch_unwind(|| get_or_try_init(&cell, || Err("allocation failed".into())));
        assert_eq!(result.unwrap().unwrap_err(), "allocation failed");
        assert!(cell.get().is_none());
    }

    #[test]
    fn failure_does_not_poison_a_later_retry() {
        let cell = OnceLock::new();
        assert!(get_or_try_init(&cell, || Err::<usize, _>("temporary failure".into())).is_err());
        assert_eq!(*get_or_try_init(&cell, || Ok(42)).unwrap(), 42);
    }

    #[test]
    fn successful_value_is_reused_without_reinitialization() {
        let cell = OnceLock::new();
        assert_eq!(*get_or_try_init(&cell, || Ok(7)).unwrap(), 7);
        assert_eq!(*get_or_try_init(&cell, || panic!("must not initialize twice")).unwrap(), 7);
    }

    #[test]
    fn losing_concurrent_value_is_dropped_without_replacing_winner() {
        struct Value(usize, Arc<AtomicUsize>);
        impl Drop for Value {
            fn drop(&mut self) { self.1.fetch_add(1, Ordering::SeqCst); }
        }
        let cell = OnceLock::new();
        let drops = Arc::new(AtomicUsize::new(0));
        let value = get_or_try_init(&cell, || {
            let _ = cell.set(Value(1, drops.clone()));
            Ok(Value(2, drops.clone()))
        }).unwrap();
        assert_eq!(value.0, 1);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}
