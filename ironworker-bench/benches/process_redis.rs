use std::error::Error;

use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::{criterion_group, criterion_main};

use ironworker_core::IntoTask;
use ironworker_core::IronworkerApplicationBuilder;
use ironworker_core::Message;
use ironworker_core::PerformableTask;
use ironworker_redis::RedisBroker;

fn bench_task(_payload: Message<usize>) -> Result<(), Box<dyn Error + Send>> {
    Ok(())
}

fn from_elem(c: &mut Criterion) {
    let size: usize = 4096;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .enable_all()
        .build()
        .unwrap();

    let app = rt.block_on(async {
        IronworkerApplicationBuilder::default()
            .broker(RedisBroker::new("redis://localhost:6379").await)
            .register_task(bench_task.task())
            .build()
    });

    c.bench_with_input(BenchmarkId::new("enqueue", size), &size, |b, &s| {
        // Insert a call to `to_async` to convert the bencher to async mode.
        // The timing loops are the same as with the normal bencher.
        b.to_async(&rt).iter(|| async {
            bench_task.task().perform_later(&app, s).await.unwrap();
        });
    });

    c.bench_function("dequeue", |b| {
        rt.block_on(async {
            for val in 0..10_000 {
                bench_task.task().perform_later(&app, val).await.unwrap();
            }
        });

        b.to_async(&rt).iter(|| async {});
    });
}

criterion_group!(benches, from_elem);
criterion_main!(benches);
