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
use tracing::{instrument, warn};

use crate::ProcessEnd;

#[derive(Debug)]
pub struct TokioCommand {
    command: Command,
}

impl TokioCommand {
    pub fn new<S: AsRef<OsStr>>(program: S) -> Self {
        Self {
            command: Command::new(program),
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

        let (end_tx, end_rx) = oneshot::channel::<ProcessEnd>();
        let end_handler = tokio::spawn(async move {
            let end_msg = match child.wait().await {
                Ok(status) => {
                    if let Some(0) = status.code() {
                        ProcessEnd::Success
                    } else {
                        let err = Self::read_full_stderr_if_any(stderr).await.err();
                        if let Some(code) = status.code() {
                            ProcessEnd::Failed(code, err)
                        } else {
                            ProcessEnd::Killed(err)
                        }
                    }
                }
                Err(err) => ProcessEnd::Error(err),
            };

            if let Err(err) = end_tx.send(end_msg) {
                warn!(?err, "receiver dropped before sending end msg");
            }
        });

        Ok(TokioCommandProcess {
            stdin,
            stdout,
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

    async fn read_full_stderr_if_any(mut stderr: impl AsyncRead + Unpin) -> Result<()> {
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
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct TokioCommandProcess {
    #[derivative(Debug = "ignore")]
    pub stdin: Box<dyn AsyncWrite + Send + Unpin>,
    #[derivative(Debug = "ignore")]
    pub stdout: Box<dyn AsyncRead + Send + Unpin>,
    pub pid: u32,
    pub end_rx: Receiver<ProcessEnd>,
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
