use ironworker_core::SerializableMessage;
use redis::{ErrorKind, FromRedisValue, RedisError, RedisResult, RedisWrite, ToRedisArgs, Value};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct RedisMessage(SerializableMessage);

impl From<SerializableMessage> for RedisMessage {
    fn from(message: SerializableMessage) -> Self {
        Self(message)
    }
}

impl From<RedisMessage> for SerializableMessage {
    fn from(message: RedisMessage) -> SerializableMessage {
        message.0
    }
}

impl ToRedisArgs for RedisMessage {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        out.write_arg(serde_json::to_string(self).unwrap().as_bytes())
    }
}

impl FromRedisValue for RedisMessage {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        match &*v {
            Value::Data(_) => {
                let out = String::from_redis_value(v)?;
                let deserialized = serde_json::from_str(&out)
                    .map_err(|_| -> RedisError { (ErrorKind::TypeError, "invalid type").into() })?;
                Ok(deserialized)
            }
            _ => Err((ErrorKind::TypeError, "invalid type").into()),
        }
    }
}
