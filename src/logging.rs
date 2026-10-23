use std::{fmt::Display, io::Write, net::IpAddr, thread, time::Duration};

use actix_web::cookie::time::UtcDateTime;
use tokio::sync::{OnceCell, mpsc::{UnboundedSender, unbounded_channel}};

use crate::cache::UuidKey;

pub enum LogMessage {
    TimeElapsed {
        elapsed: Duration,
        name: &'static str,
    },
    ElapsedUserStatus {
        key: UuidKey,
        elapsed: Duration,
        message: &'static str,
        code: u16,
    },
    /// How long something for `key` took, and how many bytes it was.
    ElapsedUserSize {
        key: UuidKey,
        elapsed: Duration,
        message: &'static str,
        size: usize,
    },
    MessageAndUser {
        key: UuidKey,
        message: &'static str,
    },
    /// Something that happened for `key` on behalf of `client`.
    ClientMessage {
        key: UuidKey,
        client: IpAddr,
        message: &'static str,
    },
    KeyError {
        key: UuidKey,
        message: &'static str,
        error: String,
    },
    /// Every request that was answered, so duplicates and who made them can be found later.
    Request {
        client: IpAddr,
        method: String,
        path: String,
        status: u16,
        elapsed: Duration,
    },
}

impl Display for LogMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TimeElapsed { elapsed, name} => {
                write!(f, "Time elapsed for {name}: {elapsed:?}")
            }
            Self::ElapsedUserStatus { key, elapsed, message, code } => {
                write!(f, "{message}: {key} ({code}; {elapsed:?})")
            }
            Self::ElapsedUserSize { key, elapsed, message, size } => {
                write!(f, "{message}: {key} ({size} bytes; {elapsed:?})")
            }
            Self::MessageAndUser { key, message: field } => {
                write!(f, "{field}: {key}")
            }
            Self::ClientMessage { key, client, message } => {
                write!(f, "{message}: {key} (client {client})")
            }
            Self::KeyError { key, message, error } => {
                write!(f, "{message}: {key} ({error})")
            }
            Self::Request { client, method, path, status, elapsed } => {
                write!(f, "Request: {client} {method} {path} -> {status} ({elapsed:?})")
            }
        }
    } 
}

static SENDER: OnceCell<UnboundedSender<(UtcDateTime, LogMessage)>> = OnceCell::const_new();

pub fn log(msg: LogMessage) {
    let _ = SENDER.get().expect("Logger should be initialized").send((UtcDateTime::now(), msg));
}

/// Logs are written by their own thread, in batches: one write for everything that queued up while the last one was writing, so
/// handling a request never waits on the output and a burst of requests doesn't turn into a write each.
pub fn init() {
    let (tx, mut rx) = unbounded_channel::<(UtcDateTime, LogMessage)>();
    SENDER.set(tx).unwrap();
    thread::spawn(move || {
        let mut batch = Vec::new();
        while let Some((time, msg)) = rx.blocking_recv() {
            let _ = writeln!(batch, "{time}: {msg}");
            while let Ok((time, msg)) = rx.try_recv() {
                let _ = writeln!(batch, "{time}: {msg}");
            }
            let _ = std::io::stdout().write_all(&batch);
            batch.clear();
        }
    });
}
