create table if not exists ironworker_jobs (
    id text not null primary key,
    task text not null,
    payload text not null,
    completed boolean not null default false,
    "queue" text,
    enqueued_at timestamp with time zone
);

create table if not exists ironworker_workers (
    id text not null primary key,
    last_seen_at timestamp with time zone not null
);

create table if not exists ironworker_reserved_jobs (
    id bigserial not null primary key,
    worker_id text not null references ironworker_workers (id),
    job_id text not null references ironworker_jobs (id)
);