use std::any::{Any, TypeId};
use std::fmt::Debug;

#[derive(PartialEq, Eq)]
pub struct TaggedError {
    pub(crate) type_id: TypeId,
    pub(crate) repr: String, // TODO: Make this back into a Box<dyn Error> at some point, I'd like to have the original representation stick around
}

impl<Err: Any + Debug + Send + 'static> From<Err> for TaggedError {
    fn from(err: Err) -> Self {
        Self {
            type_id: TypeId::of::<Err>(),
            repr: format!("{:?}", err),
        }
    }
}
