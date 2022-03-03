use actix_web::{
    middleware::Logger,
    web::{self, Json},
    App, HttpResponse, HttpServer,
};
use async_trait::async_trait;
use ironworker::{
    application::{IronworkerApplication, IronworkerApplicationBuilder},
    extract::{AddMessageStateMiddleware, Extract},
    message::{Message, SerializableMessage},
    middleware::IronworkerMiddleware,
    redis::RedisBroker,
    task::{IntoTask, Task},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    convert::Infallible,
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, RwLock,
    },
};
use uuid::Uuid;

static COUNTER: AtomicUsize = AtomicUsize::new(0);

struct CounterMiddleware;

#[async_trait]
impl IronworkerMiddleware for CounterMiddleware {
    async fn after_perform(&self, _: &SerializableMessage) {
        COUNTER.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::main]
async fn main() {
    // Set the RUST_LOG, if it hasn't been explicitly defined
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));
    // tracing_subscriber::fmt::init();

    let db = Db::default();

    let ironworker = IronworkerApplicationBuilder::default()
        .broker(RedisBroker::new("redis://localhost:6379").await.unwrap())
        .register_task(log_todos.task().retries(2))
        .register_middleware(AddMessageStateMiddleware::new(db.clone()))
        .build();

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    tracing::debug!("listening on {}", addr);

    let worker = ironworker.clone();
    tokio::spawn(async move {
        worker.run().await;
    });

    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .service(ironworker::actix::endpoints(ironworker.clone()))
            .service(
                web::resource("/todos")
                    .route(web::get().to(todos_index))
                    .route(web::post().to(todos_create)),
            )
            .service(
                web::resource("/todos/{id}")
                    .route(web::patch().to(todos_update))
                    .route(web::delete().to(todos_delete)),
            )
            .service(web::resource("/count").route(web::get().to(log_count)))
            .app_data(web::Data::new(db.clone()))
            .app_data(ironworker.clone())
    })
    .bind(addr)
    .unwrap()
    .workers(2)
    .run()
    .await
    .unwrap();
}

// The query parameters for todos index
#[derive(Debug, Deserialize, Default)]
pub struct Pagination {
    pub offset: Option<usize>,
    pub limit: Option<usize>,
}

async fn todos_index(
    pagination: Option<web::Query<Pagination>>,
    db: web::Data<Db>,
) -> Json<Vec<Todo>> {
    let todos = db.read().unwrap();

    let pagination: Pagination = if let Some(pagination) = pagination {
        pagination.into_inner()
    } else {
        Default::default()
    };

    let todos = todos
        .values()
        .cloned()
        .skip(pagination.offset.unwrap_or(0))
        .take(pagination.limit.unwrap_or(usize::MAX))
        .collect::<Vec<_>>();

    Json(todos)
}

#[derive(Debug, Deserialize)]
struct CreateTodo {
    text: String,
}

async fn todos_create(
    Json(input): Json<CreateTodo>,
    db: web::Data<Db>,
    ironworker: web::Data<IronworkerApplication<RedisBroker>>,
) -> HttpResponse {
    let ironworker = ironworker.into_inner();
    let todo = Todo {
        id: Uuid::new_v4(),
        text: input.text,
        completed: false,
    };

    db.write().unwrap().insert(todo.id, todo.clone());
    log_todos
        .task()
        .wait(chrono::Duration::seconds(30))
        .perform_later(&*ironworker, todo.id)
        .await
        .unwrap();

    HttpResponse::Created().json(todo)
}

#[derive(Debug, Deserialize)]
struct UpdateTodo {
    text: Option<String>,
    completed: Option<bool>,
}

async fn todos_update(
    path: web::Path<Uuid>,
    Json(input): Json<UpdateTodo>,
    db: web::Data<Db>,
) -> HttpResponse {
    let id = path.into_inner();
    let todo = db.read().unwrap().get(&id).cloned();

    if todo.is_none() {
        return HttpResponse::NotFound().finish();
    }

    let mut todo = todo.unwrap();

    if let Some(text) = input.text {
        todo.text = text;
    }

    if let Some(completed) = input.completed {
        todo.completed = completed;
    }

    db.write().unwrap().insert(todo.id, todo.clone());

    HttpResponse::Ok().json(todo)
}

async fn todos_delete(path: web::Path<Uuid>, db: web::Data<Db>) -> HttpResponse {
    let id = path.into_inner();
    if db.write().unwrap().remove(&id).is_some() {
        HttpResponse::NoContent().finish()
    } else {
        HttpResponse::NotFound().finish()
    }
}

async fn log_todos(Message(id): Message<Uuid>, Extract(db): Extract<Db>) -> Result<(), Infallible> {
    let todos = db.read().unwrap();
    let todo = todos.values().cloned().find(|val| val.id == id);
    dbg!(todo);
    Ok(())
}

async fn log_count() -> String {
    format!("{}", COUNTER.load(Ordering::SeqCst))
}

type Db = Arc<RwLock<HashMap<Uuid, Todo>>>;

#[derive(Debug, Serialize, Clone)]
struct Todo {
    id: Uuid,
    text: String,
    completed: bool,
}
