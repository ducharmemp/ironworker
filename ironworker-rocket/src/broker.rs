use ironworker_redis::RedisBroker;
use rocket::{
    http::Status,
    request::{FromRequest, Outcome, Request},
};

pub struct Broker<'a>(pub RedisBroker<'a>);

#[async_trait]
impl<'r> FromRequest<'r> for &'r Broker<'r> {
    type Error = ();

    async fn from_request(request: &'r Request<'_>) -> Outcome<Self, Self::Error> {
        if let Some(broker) = request.rocket().state::<Broker>() {
            return Outcome::Success(broker);
        } else {
            Outcome::Failure((Status::InternalServerError, ()))
        }
    }
}
