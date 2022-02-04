#[derive(Debug)]
pub(crate) struct Queue {
    pub(crate) url: String,
}

impl Queue {
    pub(crate) fn new(url: String) -> Self {
        Self { url }
    }
}
