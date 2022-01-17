use testcontainers::{Container, Docker, Image, WaitForMessage};
use std::collections::HashMap;

const CONTAINER_IDENTIFIER: &str = "roribio16/alpine-sqs";
const DEFAULT_TAG: &str = "1.2.0";

#[derive(Debug, Default, Clone)]
pub struct SqsArgs;

impl IntoIterator for SqsArgs {
    type Item = String;
    type IntoIter = ::std::vec::IntoIter<String>;

    fn into_iter(self) -> <Self as IntoIterator>::IntoIter {
        vec![].into_iter()
    }
}

#[derive(Debug)]
pub struct Sqs {
    tag: String,
    arguments: SqsArgs,
}

impl Default for Sqs {
    fn default() -> Self {
        Sqs {
            tag: DEFAULT_TAG.to_string(),
            arguments: SqsArgs {},
        }
    }
}

impl Image for Sqs {
    type Args = SqsArgs;
    type EnvVars = HashMap<String, String>;
    type Volumes = HashMap<String, String>;
    type EntryPoint = std::convert::Infallible;

    fn descriptor(&self) -> String {
        format!("{}:{}", CONTAINER_IDENTIFIER, &self.tag)
    }

    fn wait_until_ready<D: Docker>(&self, container: &Container<'_, D, Self>) {
        container
            .logs()
            .stdout
            .wait_for_message("listening on port 9325")
            .unwrap();
    }

    fn args(&self) -> <Self as Image>::Args {
        self.arguments.clone()
    }

    fn volumes(&self) -> Self::Volumes {
        HashMap::new()
    }

    fn env_vars(&self) -> Self::EnvVars {
        HashMap::new()
    }

    fn with_args(self, arguments: <Self as Image>::Args) -> Self {
        Sqs { arguments, ..self }
    }
}

impl Sqs {
    pub fn with_tag(self, tag_str: &str) -> Self {
        Sqs {
            tag: tag_str.to_string(),
            ..self
        }
    }
}
