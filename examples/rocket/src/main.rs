#[macro_use]
extern crate rocket;
#[macro_use]
extern crate rocket_sync_db_pools;
#[macro_use]
extern crate diesel_migrations;
#[macro_use]
extern crate diesel;

use std::sync::Arc;

use ironworker_core::{
    IntoTask, IronworkerApplication, IronworkerApplicationBuilder, Message, PerformableTask,
};
use ironworker_redis::RedisBroker;
use ironworker_rocket::IronworkerFairing;
use rocket::fairing::AdHoc;
use rocket::response::{status::Created, Debug};
use rocket::serde::{json::Json, Deserialize, Serialize};
use rocket::{Build, Rocket, State};
use snafu::Snafu;
use diesel::prelude::*;

#[derive(Snafu, Debug)]
enum TestEnum {
    #[snafu(display("The task failed"))]
    Failed,
}

#[database("diesel")]
struct Db(diesel::SqliteConnection);

type Result<T, E = Debug<diesel::result::Error>> = std::result::Result<T, E>;

#[derive(Debug, Clone, Deserialize, Serialize, Queryable, Insertable)]
#[serde(crate = "rocket::serde")]
#[table_name = "posts"]
struct Post {
    #[serde(skip_deserializing)]
    id: Option<i32>,
    title: String,
    text: String,
    #[serde(skip_deserializing)]
    published: bool,
}

table! {
    posts (id) {
        id -> Nullable<Integer>,
        title -> Text,
        text -> Text,
        published -> Bool,
    }
}

#[post("/", data = "<post>")]
async fn create(db: Db, post: Json<Post>) -> Result<Created<Json<Post>>> {
    let post_value = post.clone();
    db.run(move |conn| {
        diesel::insert_into(posts::table)
            .values(&post_value)
            .execute(conn)
    })
    .await?;

    Ok(Created::new("/").body(post))
}

fn create_posts(_payload: Message<Post>) -> Result<(), TestEnum> {
    // futures::executor::block_on(async {
    //     pool.run(|conn| async move {
    //         diesel::insert_into(posts::table)
    //             .values(&payload.into_inner())
    //             .execute(&conn)
    //             .unwrap();
    //     })
    //     .await
    // });
    Ok(())
}

#[get("/")]
async fn list(
    db: Db,
    ironworker: &State<Arc<IronworkerApplication<RedisBroker>>>,
) -> Result<Json<Vec<Option<i32>>>> {
    let ids: Vec<Option<i32>> = db
        .run(move |conn| posts::table.select(posts::id).load(conn))
        .await?;

    let p = Post {
        id: None,
        published: false,
        text: "This is a test".to_string(),
        title: "Test title".to_string(),
    };
    create_posts.task().perform_later(ironworker, p).await;

    Ok(Json(ids))
}

#[get("/<id>")]
async fn read(db: Db, id: i32) -> Option<Json<Post>> {
    db.run(move |conn| posts::table.filter(posts::id.eq(id)).first(conn))
        .await
        .map(Json)
        .ok()
}

#[delete("/<id>")]
async fn delete(db: Db, id: i32) -> Result<Option<()>> {
    let affected = db
        .run(move |conn| {
            diesel::delete(posts::table)
                .filter(posts::id.eq(id))
                .execute(conn)
        })
        .await?;

    Ok((affected == 1).then(|| ()))
}

#[delete("/")]
async fn destroy(db: Db) -> Result<()> {
    db.run(move |conn| diesel::delete(posts::table).execute(conn))
        .await?;

    Ok(())
}

async fn run_migrations(rocket: Rocket<Build>) -> Rocket<Build> {
    // This macro from `diesel_migrations` defines an `embedded_migrations`
    // module containing a function named `run` that runs the migrations in the
    // specified directory, initializing the database.
    embed_migrations!("migrations");

    let conn = Db::get_one(&rocket).await.expect("database connection");
    conn.run(|c| embedded_migrations::run(c))
        .await
        .expect("diesel migrations");

    rocket
}

pub async fn stage() -> AdHoc {
    let app = IronworkerApplicationBuilder::default()
        .broker(RedisBroker::new("redis://localhost:6379").await)
        .register_task(create_posts.task())
        .build();

    AdHoc::on_ignite("Diesel SQLite Stage", |rocket| async {
        rocket
            .attach(Db::fairing())
            .attach(AdHoc::on_ignite("Diesel Migrations", run_migrations))
            .attach(IronworkerFairing::new("/queues", app))
            .mount("/diesel", routes![list, read, create, delete, destroy])
    })
}

#[launch]
async fn rocket() -> _ {
    tracing_subscriber::fmt::init();
    rocket::build().attach(stage().await)
}
