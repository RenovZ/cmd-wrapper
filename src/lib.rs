mod std_command;
mod tokio_command;

pub use std_command::*;
use tokio::io;
pub use tokio_command::*;

#[derive(Debug)]
pub enum ProcessEnd {
    Success,
    Failed(i32, Option<io::Error>),
    Killed(Option<io::Error>),
    Error(std::io::Error),
}
