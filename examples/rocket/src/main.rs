extern crate rocket;

use ironworker_rocket::IronworkerFairing;

#[rocket::launch]
fn rocket() -> _ {
    rocket::build().attach(IronworkerFairing::new("/queues"))
}
