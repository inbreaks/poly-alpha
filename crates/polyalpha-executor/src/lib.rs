pub mod dry_run;
pub mod manager;

pub use dry_run::DryRunExecutor;
pub use manager::ExecutionManager;

pub fn crate_status() -> &'static str {
    "polyalpha-executor dry-run ready"
}
