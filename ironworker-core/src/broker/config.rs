#[derive(Clone, Copy, Debug)]
pub enum RetryStrategy {
    Manual,
    Automatic,
}

impl RetryStrategy {
    /// Returns `true` if the retry strategy is [`Automatic`].
    ///
    /// [`Automatic`]: RetryStrategy::Automatic
    pub fn is_automatic(&self) -> bool {
        matches!(self, Self::Automatic)
    }

    /// Returns `true` if the retry strategy is [`Manual`].
    ///
    /// [`Manual`]: RetryStrategy::Manual
    pub fn is_manual(&self) -> bool {
        matches!(self, Self::Manual)
    }
}

#[derive(Clone, Copy, Debug)]
pub enum HeartbeatStrategy {
    NoHeartbeat,
    Periodic,
}

impl HeartbeatStrategy {
    /// Returns `true` if the heartbeat strategy is [`NoHeartbeat`].
    ///
    /// [`NoHeartbeat`]: HeartbeatStrategy::NoHeartbeat
    pub fn is_no_heartbeat(&self) -> bool {
        matches!(self, Self::NoHeartbeat)
    }

    /// Returns `true` if the heartbeat strategy is [`Periodic`].
    ///
    /// [`Periodic`]: HeartbeatStrategy::Periodic
    pub fn is_periodic(&self) -> bool {
        matches!(self, Self::Periodic)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct BrokerConfig {
    pub retry_strategy: RetryStrategy,
    pub heartbeat_strategy: HeartbeatStrategy,
}
