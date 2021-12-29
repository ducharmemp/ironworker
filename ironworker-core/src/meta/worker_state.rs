use chrono::{DateTime, Utc};

pub struct WorkerState {
    pub name: String,
    pub queues: Option<String>,
    pub last_seen_at: Option<DateTime<Utc>>,
}