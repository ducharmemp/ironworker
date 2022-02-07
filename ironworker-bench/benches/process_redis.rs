use std::sync::mpsc::channel;
use std::sync::Arc;
use std::time::Duration;

use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::{criterion_group, criterion_main};
use deadpool_redis::{Config, Runtime};
use ironworker_core::Task;
use redis::AsyncCommands;
use snafu::prelude::*;
use testcontainers::{clients, images, Docker};

use ironworker_core::IntoTask;
use ironworker_core::IronworkerApplicationBuilder;
use ironworker_core::Message;
use ironworker_redis::RedisBroker;

#[derive(Snafu, Debug)]
enum TestEnum {
    #[snafu(display("The task failed"))]
    Failed,
}

fn bench_task(_payload: Message<usize>) -> Result<(), TestEnum> {
    Ok(())
}

fn from_elem(c: &mut Criterion) {
    let size: usize = 4096;
    let docker = clients::Cli::default();
    let node = docker.run(images::redis::Redis::default().with_tag("latest"));
    let host_port = node.get_host_port(6379).unwrap();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .enable_all()
        .build()
        .unwrap();

    let local_client = Config::from_url(&format!("redis://localhost:{}", host_port))
        .builder()
        .unwrap()
        .max_size(10)
        .runtime(Runtime::Tokio1)
        .build()
        .unwrap();

    let mut group = c.benchmark_group("redis");
    group.sample_size(10);

    let app = rt.block_on(async {
        Arc::new(
            IronworkerApplicationBuilder::default()
                .broker(RedisBroker::new(&format!("redis://localhost:{}", host_port)).await)
                .register_task(bench_task.task())
                .build(),
        )
    });

    group.bench_with_input(BenchmarkId::new("enqueue", size), &size, |b, &s| {
        // Insert a call to `to_async` to convert the bencher to async mode.
        // The timing loops are the same as with the normal bencher.
        b.to_async(&rt).iter(|| async {
            bench_task.task().perform_later(&app, s).await.unwrap();
        });
    });

    group.bench_function("dequeue", |b| {
        let local_client = local_client.clone();

        b.to_async(&rt).iter_batched(
            || {
                let local_client = local_client.clone();
                let (tx, rx) = channel();
                tokio::task::spawn(async move {
                    let mut conn = local_client.get().await.unwrap();
                    conn.del::<&str, ()>("queue:default").await.unwrap();

                    let app = Arc::new(
                        IronworkerApplicationBuilder::default()
                            .broker(
                                RedisBroker::new(&format!("redis://localhost:{}", host_port)).await,
                            )
                            .register_task(bench_task.task())
                            .build(),
                    );
                    for val in 0..100 {
                        bench_task.task().perform_later(&app, val).await.unwrap();
                    }
                    tx.send(app).unwrap();
                });

                rx.recv().unwrap()
            },
            |outer_app| {
                let local_client = local_client.clone();
                async move {
                    let mut conn = local_client.get().await.unwrap();
                    let app = outer_app.clone();
                    let mut default_queue_len =
                        conn.llen::<&str, u64>("queue:default").await.unwrap();
                    assert_eq!(default_queue_len, 100);
                    tokio::task::spawn(async move { app.run().await });
                    while default_queue_len != 0 {
                        default_queue_len = conn.llen::<&str, u64>("queue:default").await.unwrap();
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    outer_app.shutdown();
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

criterion_group!(benches, from_elem);
criterion_main!(benches);
