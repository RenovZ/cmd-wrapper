use std::{
    ffi::OsStr,
    io::{self, Error, Read, Result, Write},
    path::Path,
    process::{Child, Command, ExitStatus, Stdio},
    sync::mpsc::{self, Receiver},
    thread::{self, JoinHandle},
};

use tracing::{error, instrument, warn};

#[derive(Debug)]
pub struct StdCommand {
    command: Command,
}

impl StdCommand {
    pub fn new<S: Into<String>>(program: S) -> Self {
        Self {
            command: Command::new(program.into()),
        }
    }

    pub fn arg<S: AsRef<OsStr>>(&mut self, arg: S) -> &mut Self {
        self.command.arg(arg);
        self
    }

    pub fn args<I, S>(&mut self, args: I) -> &mut Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        self.command.args(args);
        self
    }

    pub fn env<K, V>(&mut self, key: K, val: V) -> &mut Self
    where
        K: AsRef<OsStr>,
        V: AsRef<OsStr>,
    {
        self.command.env(key, val);
        self
    }

    pub fn envs<I, K, V>(&mut self, vars: I) -> &mut Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<OsStr>,
        V: AsRef<OsStr>,
    {
        self.command.envs(vars);
        self
    }

    pub fn current_dir<P: AsRef<Path>>(&mut self, dir: P) -> &mut Self {
        self.command.current_dir(dir);
        self
    }

    pub fn stdin<T: Into<Stdio>>(&mut self, stdin: T) -> &mut Self {
        self.command.stdin(stdin);
        self
    }

    pub fn stdout<T: Into<Stdio>>(&mut self, stdout: T) -> &mut Self {
        self.command.stdout(stdout);
        self
    }

    pub fn stderr<T: Into<Stdio>>(&mut self, stderr: T) -> &mut Self {
        self.command.stderr(stderr);
        self
    }

    #[instrument(level = "trace")]
    pub fn into_inner(self) -> Command {
        self.command
    }

    #[instrument(level = "trace")]
    pub fn child(mut self) -> Result<Child> {
        self.command.spawn()
    }

    #[instrument(level = "trace")]
    pub fn spawn(&mut self) -> Result<StdProcessContext> {
        let mut child = self.command.spawn()?;

        let stdin = child
            .stdin
            .take()
            .map(|s| Box::new(s) as Box<dyn Write>)
            .unwrap_or_else(|| Box::new(SyncSink));
        let stdout = child
            .stdout
            .take()
            .map(|s| Box::new(s) as Box<dyn Read>)
            .unwrap_or_else(|| Box::new(io::empty()));
        let stderr = child
            .stderr
            .take()
            .map(|s| Box::new(s) as Box<dyn Read>)
            .unwrap_or_else(|| Box::new(io::empty()));

        let pid = child.id();

        let (end_tx, end_rx) = mpsc::channel::<i32>();
        let end_handler = thread::spawn(move || {
            let exit_code = match child.wait() {
                Ok(status) if status.code() == Some(0) => 0,
                Ok(_) => 1,
                Err(err) => {
                    error!("waiting for child process failed: {err}");
                    -1
                }
            };

            if let Err(err) = end_tx.send(exit_code) {
                warn!("receiver dropped before sending exit code: {err}");
            }
        });

        Ok(StdProcessContext {
            stdin,
            stdout,
            stderr,
            pid,
            end_rx,
            end_handler,
        })
    }

    #[instrument(level = "trace")]
    pub fn output(&mut self) -> Result<String> {
        let output = self.command.output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::other(stderr));
        }

        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }

    #[instrument(level = "trace")]
    pub fn status(&mut self) -> Result<ExitStatus> {
        self.command.status()
    }

    #[instrument(level = "trace")]
    pub fn execute_and_print(&mut self) -> Result<()> {
        let output = self.command.output()?;
        if !output.stdout.is_empty() {
            io::stdout().write_all(&output.stdout)?;
        }
        if !output.stderr.is_empty() {
            io::stderr().write_all(&output.stderr)?;
        }
        Ok(())
    }

    pub fn read_full_stderr_if_any(stderr: &mut impl Read) -> Result<()> {
        let mut peek_buf = vec![0u8; 1024];
        let n = stderr.read(&mut peek_buf)?;
        if n == 0 {
            return Ok(());
        }

        let mut full = peek_buf[..n].to_vec();
        let mut rest = Vec::new();
        stderr.read_to_end(&mut rest)?;
        full.extend(rest);

        let msg = String::from_utf8_lossy(&full).trim().to_string();
        if !msg.is_empty() {
            return Err(Error::other(msg));
        }

        Ok(())
    }
}

pub struct StdProcessContext {
    pub stdin: Box<dyn Write>,
    pub stdout: Box<dyn Read>,
    pub stderr: Box<dyn Read>,
    pub pid: u32,
    pub end_rx: Receiver<i32>,
    pub end_handler: JoinHandle<()>,
}

struct SyncSink;

impl Write for SyncSink {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
