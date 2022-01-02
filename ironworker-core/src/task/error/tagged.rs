use std::any::{Any, TypeId};
use std::fmt::Debug;

pub struct TaggedError {
    pub(crate) type_id: TypeId,
    pub(crate) wrapped: Box<dyn Debug + Send + 'static>,
}

impl<Err: Any + Debug + Send + 'static> From<Err> for TaggedError {
    fn from(err: Err) -> Self {
        Self {
            type_id: TypeId::of::<Err>(),
            wrapped: Box::new(err),
        }
    }
}
