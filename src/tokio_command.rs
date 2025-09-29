use std::{
    ffi::OsStr,
    io::{Error, Result},
    path::Path,
    pin::Pin,
    process::{ExitStatus, Stdio},
    task::{Context, Poll},
};

use derivative::Derivative;
use tokio::{
    io::{self, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    process::{Child, Command},
    sync::oneshot::{self, Receiver},
    task::JoinHandle,
};
use tracing::{error, instrument, warn};

#[derive(Debug)]
pub struct TokioCommand {
    command: Command,
}

impl TokioCommand {
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
    pub async fn spawn(&mut self) -> Result<TokioCommandProcess> {
        let mut child = self.command.spawn()?;

        let stdin = child
            .stdin
            .take()
            .map(|s| Box::new(s) as Box<dyn AsyncWrite + Send + Unpin>)
            .unwrap_or_else(|| Box::new(AsyncSink));
        let stdout = child
            .stdout
            .take()
            .map(|s| Box::new(s) as Box<dyn AsyncRead + Send + Unpin>)
            .unwrap_or_else(|| Box::new(io::empty()));
        let stderr = child
            .stderr
            .take()
            .map(|s| Box::new(s) as Box<dyn AsyncRead + Send + Unpin>)
            .unwrap_or_else(|| Box::new(io::empty()));

        let pid = child.id().unwrap_or_default();

        let (end_tx, end_rx) = oneshot::channel::<i32>();
        let end_handler = tokio::spawn(async move {
            let exit_code = match child.wait().await {
                Ok(status) if status.code() == Some(0) => 0,
                Ok(status) => status.code().unwrap_or(-1),
                Err(err) => {
                    error!("waiting for child process failed: {err}");
                    -1
                }
            };

            if let Err(err) = end_tx.send(exit_code) {
                warn!("receiver dropped before sending exit code: {err}");
            }
        });

        Ok(TokioCommandProcess {
            stdin,
            stdout,
            stderr,
            pid,
            end_rx,
            end_handler,
        })
    }

    #[instrument(level = "trace")]
    pub async fn output(&mut self) -> Result<String> {
        let output = self.command.output().await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::other(stderr));
        }

        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }

    #[instrument(level = "trace")]
    pub async fn status(&mut self) -> Result<ExitStatus> {
        self.command.status().await
    }

    #[instrument(level = "trace")]
    pub async fn execute_and_print(&mut self) -> Result<()> {
        let output = self.command.output().await?;
        if !output.stdout.is_empty() {
            io::stdout().write_all(&output.stdout).await?;
        }
        if !output.stderr.is_empty() {
            io::stderr().write_all(&output.stderr).await?;
        }
        Ok(())
    }

    pub async fn read_full_stderr_if_any(stderr: &mut (impl AsyncRead + Unpin)) -> Result<()> {
        let mut peek_buf = vec![0u8; 1024];
        let n = stderr.read(&mut peek_buf).await?;
        if n == 0 {
            return Ok(());
        }

        let mut full = peek_buf[..n].to_vec();
        let mut rest = Vec::new();
        stderr.read_to_end(&mut rest).await?;
        full.extend(rest);

        let msg = String::from_utf8_lossy(&full).trim().to_string();
        if !msg.is_empty() {
            return Err(Error::other(msg));
        }

        Ok(())
    }

    pub async fn read_bytes_with_stderr_check(
        stdout: &mut (impl AsyncRead + Unpin),
        stderr: &mut (impl AsyncRead + Unpin),
    ) -> Result<Option<Vec<u8>>> {
        let mut out_buf = vec![0u8; 32 << 10];
        let mut err_buf = vec![0u8; 1024];

        loop {
            tokio::select! {
                read_stdout = stdout.read(&mut out_buf) => {
                    let n = read_stdout?;
                    if n == 0 {
                        return Ok(None);
                    }
                    out_buf.truncate(n);
                    return Ok(Some(out_buf))
                }
                read_stderr = stderr.read(&mut err_buf) => {
                    let n = read_stderr?;
                    if n > 0 {
                        let msg = str::from_utf8(&err_buf[..n]).map_err(Error::other)?.trim();
                        if !msg.is_empty() {
                            return Err(Error::other(msg));
                        }
                    }
                }
            }
        }
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct TokioCommandProcess {
    #[derivative(Debug = "ignore")]
    pub stdin: Box<dyn AsyncWrite + Send + Unpin>,
    #[derivative(Debug = "ignore")]
    pub stdout: Box<dyn AsyncRead + Send + Unpin>,
    #[derivative(Debug = "ignore")]
    pub stderr: Box<dyn AsyncRead + Send + Unpin>,
    pub pid: u32,
    pub end_rx: Receiver<i32>,
    pub end_handler: JoinHandle<()>,
}

struct AsyncSink;

impl AsyncWrite for AsyncSink {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Poll::Ready(Ok(buf.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
