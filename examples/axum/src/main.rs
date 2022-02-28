//! Provides a RESTful web server managing some Todos.
//!
//! API will be:
//!
//! - `GET /todos`: return a JSON list of Todos.
//! - `POST /todos`: create a new Todo.
//! - `PUT /todos/:id`: update a specific Todo.
//! - `DELETE /todos/:id`: delete a specific Todo.
//!
//! Run with
//!
//! ```not_rust
//! cargo run -p example-todos
//! ```
//!
//! Taken from <https://github.com/tokio-rs/axum/blob/main/examples/todos/src/main.rs> with ironworker mixed in

use async_trait::async_trait;
use axum::{
    error_handling::HandleErrorLayer,
    extract::{Extension, Path, Query},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, patch},
    Json, Router,
};
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
    time::Duration,
};
use tower::{BoxError, ServiceBuilder};
use tower_http::{add_extension::AddExtensionLayer, trace::TraceLayer};
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
    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var("RUST_LOG", "example_todos=debug,tower_http=debug")
    }
    // tracing_subscriber::fmt::init();

    let db = Db::default();

    let ironworker = Arc::new(
        IronworkerApplicationBuilder::default()
            .broker(RedisBroker::new("redis://localhost:6379").await.unwrap())
            .register_task(log_todos.task().retries(2))
            .register_middleware(AddMessageStateMiddleware::new(db.clone()))
            .build(),
    );

    // Compose the routes
    let app = Router::new()
        .merge(ironworker::axum::endpoints(ironworker.clone()))
        .route("/todos", get(todos_index).post(todos_create))
        .route("/todos/:id", patch(todos_update).delete(todos_delete))
        .route("/count", get(log_count))
        // Add middleware to all routes
        .layer(
            ServiceBuilder::new()
                .layer(HandleErrorLayer::new(|error: BoxError| async move {
                    if error.is::<tower::timeout::error::Elapsed>() {
                        Ok(StatusCode::REQUEST_TIMEOUT)
                    } else {
                        Err((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("Unhandled internal error: {}", error),
                        ))
                    }
                }))
                .timeout(Duration::from_secs(10))
                .layer(TraceLayer::new_for_http())
                .layer(AddExtensionLayer::new(db.clone()))
                .layer(AddExtensionLayer::new(ironworker.clone()))
                .into_inner(),
        );

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    tracing::debug!("listening on {}", addr);
    tokio::spawn(async move {
        ironworker.run().await;
    });

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
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
    pagination: Option<Query<Pagination>>,
    Extension(db): Extension<Db>,
) -> impl IntoResponse {
    let todos = db.read().unwrap();

    let Query(pagination) = pagination.unwrap_or_default();

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
    Extension(db): Extension<Db>,
    Extension(ironworker): Extension<Arc<IronworkerApplication<RedisBroker>>>,
) -> impl IntoResponse {
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

    (StatusCode::CREATED, Json(todo))
}

#[derive(Debug, Deserialize)]
struct UpdateTodo {
    text: Option<String>,
    completed: Option<bool>,
}

async fn todos_update(
    Path(id): Path<Uuid>,
    Json(input): Json<UpdateTodo>,
    Extension(db): Extension<Db>,
) -> Result<impl IntoResponse, StatusCode> {
    let mut todo = db
        .read()
        .unwrap()
        .get(&id)
        .cloned()
        .ok_or(StatusCode::NOT_FOUND)?;

    if let Some(text) = input.text {
        todo.text = text;
    }

    if let Some(completed) = input.completed {
        todo.completed = completed;
    }

    db.write().unwrap().insert(todo.id, todo.clone());

    Ok(Json(todo))
}

async fn todos_delete(Path(id): Path<Uuid>, Extension(db): Extension<Db>) -> impl IntoResponse {
    if db.write().unwrap().remove(&id).is_some() {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
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
