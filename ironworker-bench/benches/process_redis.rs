use std::sync::Arc;
use std::time::Duration;

use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::{criterion_group, criterion_main};
use deadpool_redis::{Config, Runtime};
use ironworker_core::Task;
use redis::AsyncCommands;
use snafu::prelude::*;

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
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .enable_all()
        .build()
        .unwrap();

    let local_client = Config::from_url("redis://localhost:6379")
        .builder()
        .unwrap()
        .max_size(1)
        .runtime(Runtime::Tokio1)
        .build()
        .unwrap();

    let mut group = c.benchmark_group("redis");
    group.sample_size(10);

    let app = rt.block_on(async {
        Arc::new(
            IronworkerApplicationBuilder::default()
                .broker(RedisBroker::new("redis://localhost:6379").await)
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
        let app = app.clone();
        rt.block_on(async {
            for val in 0..100 {
                bench_task.task().perform_later(&app, val).await.unwrap();
            }
        });

        b.to_async(&rt).iter(|| async {
            let inner_app = app.clone();
            let mut conn = local_client.get().await.unwrap();
            let mut default_queue_len = conn.llen::<&str, u64>("queue:default").await.unwrap();
            tokio::task::spawn(async move { inner_app.run().await });
            while default_queue_len != 0 {
                default_queue_len = conn.llen::<&str, u64>("queue:default").await.unwrap();
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        });
    });
}

criterion_group!(benches, from_elem);
criterion_main!(benches);
