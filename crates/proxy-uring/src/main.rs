//! `lb-proxy-uring` — one `io_uring` data plane, two scheduler topologies.
//!
//! The whole point of putting both in one binary is that everything except the
//! scheduler is shared code: same routing, same header hygiene, same Hyper
//! HTTP/1.1 stack, same reactor, same allocator. So a difference between
//! `--scheduler ws` and `--scheduler tpc` is attributable to the scheduler, and
//! a difference between `--dispatch none` and `--dispatch balanced` is
//! attributable to connection placement alone.
//!
//! | Mode | Rings | Scheduler | Placement |
//! | --- | --- | --- | --- |
//! | `--scheduler ws` | one, shared | Tokio multi-thread, work-stealing | tasks migrate freely |
//! | `--scheduler tpc --dispatch none` | one per core | Tokio current-thread, pinned | `SO_REUSEPORT` kernel hash |
//! | `--scheduler tpc --dispatch balanced` | one per core | Tokio current-thread, pinned | least-loaded core at accept |

mod serve;

use clap::{Parser, ValueEnum};
use lb_core::{RouteConfig, SharedRouteTable, alloc};
use lb_uring::{Reactor, ReactorConfig, UringConnector, UringListener, UringStream};
use serve::{CoreLoad, Shared, build_client, pin_to_cpu, serve_connection};
use std::net::SocketAddr;
use std::os::unix::io::RawFd;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

// Same allocator as the Hyper baseline, so the allocator is not a second
// uncontrolled variable.
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Copy, Clone, PartialEq, Eq, Debug, ValueEnum)]
enum SchedulerKind {
    /// Tokio multi-thread: one shared ring, tasks stolen between cores.
    Ws,
    /// One pinned single-thread runtime and ring per core.
    Tpc,
}

#[derive(Copy, Clone, PartialEq, Eq, Debug, ValueEnum)]
enum Rings {
    /// A single ring for the whole process. Every worker contends for it.
    Shared,
    /// One ring per worker thread. A stolen task still submits to the ring its
    /// connection was accepted on, so contention is limited to migrated work.
    PerWorker,
}

#[derive(Copy, Clone, PartialEq, Eq, Debug, ValueEnum)]
enum Dispatch {
    /// Whatever core the kernel's `SO_REUSEPORT` hash chose. Share-nothing.
    None,
    /// Hand the connection to the least loaded core at accept time.
    Balanced,
}

#[derive(Parser, Debug)]
#[command(author, version, about = "io_uring reverse proxy (lb benchmark candidate)")]
struct Args {
    #[arg(short, long, default_value = "0.0.0.0:8080")]
    listen: SocketAddr,

    /// YAML route configuration.
    #[arg(short, long)]
    config: Option<String>,

    /// Fallback upstream when no --config is given.
    #[arg(short, long, default_value = "127.0.0.1:9090")]
    default_upstream: String,

    /// Worker threads / cores. Defaults to the logical CPU count.
    #[arg(long)]
    workers: Option<usize>,

    #[arg(long, value_enum, default_value_t = SchedulerKind::Ws)]
    scheduler: SchedulerKind,

    /// Connection placement. Only meaningful with `--scheduler tpc`.
    #[arg(long, value_enum, default_value_t = Dispatch::None)]
    dispatch: Dispatch,

    /// Ring topology. Only meaningful with `--scheduler ws`.
    #[arg(long, value_enum, default_value_t = Rings::PerWorker)]
    rings: Rings,

    /// Let a kernel thread drain the submission queue, so submitting an op
    /// costs no syscall.
    #[arg(long)]
    sqpoll: bool,

    /// Confine each SQPOLL kernel thread to a worker's own core. Without this
    /// the poller floats and the process quietly exceeds its core budget.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    sqpoll_pin: bool,

    /// Queue SQEs and submit them a batch at a time instead of one syscall per
    /// operation. This is the lever that decides whether io_uring beats epoll.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    defer_submit: bool,

    /// Submit inline once this many SQEs are queued.
    #[arg(long, default_value_t = 64)]
    submit_threshold: u32,

    /// Ring size, in entries. Must be a power of two.
    #[arg(long, default_value_t = 4096)]
    ring_entries: u32,

    /// Pin worker threads to cores (thread-per-core only).
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pin: bool,

    /// Log reactor syscall counters every N seconds. 0 disables.
    #[arg(long, default_value_t = 0)]
    stats_secs: u64,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    alloc::register("jemalloc");

    let args = Args::parse();
    let workers = args.workers.unwrap_or_else(num_cpus::get).max(1);

    let seed = match &args.config {
        Some(path) => RouteConfig::from_path(path)?,
        None => RouteConfig::single_upstream(&args.default_upstream),
    };
    let routes = Arc::new(SharedRouteTable::from_table(seed.compile()));
    let shared = Arc::new(Shared {
        routes,
        forwarded_headers: true,
        h1_max_buf_size: 64 * 1024,
    });

    info!(
        allocator = alloc::allocator_name(),
        listen = %args.listen,
        workers,
        scheduler = ?args.scheduler,
        dispatch = ?args.dispatch,
        rings = ?args.rings,
        sqpoll = args.sqpoll,
        defer_submit = args.defer_submit,
        "starting lb-proxy-uring"
    );

    match args.scheduler {
        SchedulerKind::Ws => run_work_stealing(&args, workers, shared),
        SchedulerKind::Tpc => run_thread_per_core(&args, workers, shared),
    }
}

/// Pooled upstream connections per host, per client. Split across cores in the
/// thread-per-core case so the total stays comparable with the shared pool.
fn pool_per_host(workers: usize, per_core: bool) -> usize {
    let total = 8_192;
    if per_core {
        (total / workers).max(64)
    } else {
        total
    }
}

/// A work-stealing runtime over one or several rings.
///
/// The ring count is the interesting knob. A single shared ring is the obvious
/// way to bolt `io_uring` onto a work-stealing scheduler and the worst one: the
/// submission queue needs a lock, and one driver task has to reap every
/// completion in the process. One ring per worker keeps the scheduler's
/// stealing while confining that contention to tasks that actually migrated.
fn run_work_stealing(args: &Args, workers: usize, shared: Arc<Shared>) -> anyhow::Result<()> {
    let ring_count = match args.rings {
        Rings::Shared => 1,
        Rings::PerWorker => workers,
    };
    let reactors: Vec<Arc<Reactor>> = (0..ring_count)
        .map(|i| {
            Reactor::new(&ReactorConfig {
                entries: args.ring_entries,
                sqpoll: args.sqpoll,
                sqpoll_idle_ms: 2_000,
                // Any worker may submit to any ring once a task is stolen, so
                // the kernel's single-issuer fast path is unavailable. That
                // restriction is part of what work-stealing costs here.
                single_issuer: false,
                sqpoll_cpu: args.sqpoll_pin.then_some(i as u32),
                defer_submit: args.defer_submit,
                submit_threshold: args.submit_threshold,
            })
        })
        .collect::<std::io::Result<_>>()?;

    // Give each worker thread a home ring, so an unmigrated task submits to a
    // ring nobody else is touching.
    let for_threads = reactors.clone();
    let next_ring = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .thread_name("lb-uring-ws")
        .on_thread_start(move || {
            let i = next_ring.fetch_add(1, Ordering::Relaxed) % for_threads.len();
            Reactor::set_current(for_threads[i].clone());
        })
        .build()?;

    runtime.block_on(async move {
        for reactor in &reactors {
            tokio::spawn(lb_uring::drive(reactor.clone()));
            tokio::spawn(lb_uring::flush_loop(reactor.clone()));
        }
        if args.stats_secs > 0 {
            spawn_stats(reactors.clone(), args.stats_secs);
        }

        // Upstream dials go to the calling thread's ring, so a request that was
        // never stolen stays on one ring end to end.
        let client = build_client(
            UringConnector::thread_local(),
            pool_per_host(workers, false),
        );
        let load = Arc::new(CoreLoad::default());

        // One `SO_REUSEPORT` accept queue per worker, exactly as the Hyper
        // candidate does, so the accept path is not a hidden difference.
        for worker in 0..workers {
            let reactor = reactors[worker % reactors.len()].clone();
            let listener = UringListener::bind_reuseport(reactor, args.listen)?;
            let shared = shared.clone();
            let client = client.clone();
            let load = load.clone();
            tokio::spawn(async move {
                loop {
                    match listener.accept().await {
                        Ok((stream, peer)) => {
                            tokio::spawn(serve_connection(
                                stream,
                                peer,
                                shared.clone(),
                                client.clone(),
                                load.clone(),
                            ));
                        }
                        Err(err) => {
                            error!(worker, %err, "accept failed");
                            tokio::task::yield_now().await;
                        }
                    }
                }
            });
        }

        info!(workers, rings = reactors.len(), "uring/work-stealing listening");
        tokio::signal::ctrl_c().await.ok();
        Ok(())
    })
}

/// One ring and one pinned single-thread runtime per core.
fn run_thread_per_core(args: &Args, workers: usize, shared: Arc<Shared>) -> anyhow::Result<()> {
    let loads: Arc<Vec<Arc<CoreLoad>>> =
        Arc::new((0..workers).map(|_| Arc::new(CoreLoad::default())).collect());

    // Handoff channels, one inbox per core. Only used by `--dispatch balanced`.
    let mut senders = Vec::with_capacity(workers);
    let mut receivers = Vec::with_capacity(workers);
    for _ in 0..workers {
        let (tx, rx) = mpsc::unbounded_channel::<(RawFd, SocketAddr)>();
        senders.push(tx);
        receivers.push(rx);
    }
    let senders = Arc::new(senders);

    let mut handles = Vec::with_capacity(workers);
    for (core, receiver) in receivers.into_iter().enumerate() {
        let args_listen = args.listen;
        let sqpoll = args.sqpoll;
        let ring_entries = args.ring_entries;
        let dispatch = args.dispatch;
        let pin = args.pin;
        let sqpoll_pin = args.sqpoll_pin;
        let defer_submit = args.defer_submit;
        let submit_threshold = args.submit_threshold;
        let stats_secs = args.stats_secs;
        let shared = shared.clone();
        let loads = loads.clone();
        let senders = senders.clone();

        handles.push(std::thread::Builder::new().name(format!("lb-uring-tpc-{core}")).spawn(
            move || -> anyhow::Result<()> {
                if pin && let Err(err) = pin_to_cpu(core) {
                    warn!(core, %err, "could not pin worker to its core");
                }

                let reactor = Reactor::new(&ReactorConfig {
                    entries: ring_entries,
                    sqpoll,
                    sqpoll_idle_ms: 2_000,
                    // Nothing else touches this ring, so the kernel can skip
                    // its cross-issuer synchronisation.
                    single_issuer: true,
                    // Keep the poller on this worker's own core: it exists to
                    // serve this ring and nothing else.
                    sqpoll_cpu: sqpoll_pin.then_some(core as u32),
                    defer_submit,
                    submit_threshold,
                })?;
                Reactor::set_current(reactor.clone());

                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?;

                runtime.block_on(async move {
                    tokio::spawn(lb_uring::drive(reactor.clone()));
                    tokio::spawn(lb_uring::flush_loop(reactor.clone()));
                    if stats_secs > 0 && core == 0 {
                        spawn_stats(vec![reactor.clone()], stats_secs);
                    }

                    let client = build_client(
                        UringConnector::shared(reactor.clone()),
                        pool_per_host(loads.len(), true),
                    );
                    let listener = UringListener::bind_reuseport(reactor.clone(), args_listen)?;
                    let buf_size = listener.buf_size();

                    // Inbox: connections other cores decided we should own.
                    tokio::spawn(handoff_loop(
                        receiver,
                        reactor.clone(),
                        buf_size,
                        shared.clone(),
                        client.clone(),
                        loads[core].clone(),
                    ));

                    loop {
                        let (fd, peer) = match listener.accept_raw().await {
                            Ok(v) => v,
                            Err(err) => {
                                error!(core, %err, "accept failed");
                                tokio::task::yield_now().await;
                                continue;
                            }
                        };

                        let target = match dispatch {
                            Dispatch::None => core,
                            Dispatch::Balanced => least_loaded(core, &loads),
                        };

                        if target == core {
                            let stream = UringStream::from_fd(reactor.clone(), fd, buf_size);
                            tokio::spawn(serve_connection(
                                stream,
                                peer,
                                shared.clone(),
                                client.clone(),
                                loads[core].clone(),
                            ));
                        } else if senders[target].send((fd, peer)).is_err() {
                            // The target core is gone; keep the connection
                            // rather than leaking the descriptor.
                            let stream = UringStream::from_fd(reactor.clone(), fd, buf_size);
                            tokio::spawn(serve_connection(
                                stream,
                                peer,
                                shared.clone(),
                                client.clone(),
                                loads[core].clone(),
                            ));
                        }
                    }
                })
            },
        )?);
    }

    info!(workers, "uring/thread-per-core listening");
    for h in handles {
        match h.join() {
            Ok(Ok(())) => {}
            Ok(Err(err)) => error!(%err, "worker exited with an error"),
            Err(_) => error!("worker panicked"),
        }
    }
    Ok(())
}

/// Serve connections other cores handed to this one.
async fn handoff_loop(
    mut inbox: mpsc::UnboundedReceiver<(RawFd, SocketAddr)>,
    reactor: Arc<Reactor>,
    buf_size: usize,
    shared: Arc<Shared>,
    client: serve::UringClient,
    load: Arc<CoreLoad>,
) {
    while let Some((fd, peer)) = inbox.recv().await {
        // The stream is built here, against *this* core's ring, which is what
        // makes the migration real rather than cosmetic.
        let stream = UringStream::from_fd(reactor.clone(), fd, buf_size);
        tokio::spawn(serve_connection(
            stream,
            peer,
            shared.clone(),
            client.clone(),
            load.clone(),
        ));
    }
}

/// Pick the core a new connection should land on.
///
/// In-flight requests dominate the score because they are the work; parked
/// keep-alive connections are nearly free and only break ties. The hysteresis
/// term stops the dispatcher trading a marginal imbalance for a cross-core
/// wakeup and a cold cache.
fn least_loaded(self_core: usize, loads: &[Arc<CoreLoad>]) -> usize {
    const REQUEST_WEIGHT: usize = 8;
    const MIGRATION_THRESHOLD: usize = 2;

    let score = |c: usize| {
        loads[c].requests.load(Ordering::Relaxed) * REQUEST_WEIGHT
            + loads[c].connections.load(Ordering::Relaxed)
    };
    let mine = score(self_core);
    let (best, best_score) = (0..loads.len())
        .map(|c| (c, score(c)))
        .min_by_key(|&(_, s)| s)
        .unwrap_or((self_core, mine));

    if best != self_core && best_score + MIGRATION_THRESHOLD <= mine {
        best
    } else {
        self_core
    }
}

/// Periodically log how much syscall batching the ring actually achieved.
fn spawn_stats(reactors: Vec<Arc<Reactor>>, secs: u64) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(secs));
        loop {
            ticker.tick().await;
            for (i, r) in reactors.iter().enumerate() {
                let (submitted, enters, completed, reaps) = r.stats_snapshot();
                info!(
                    ring = i,
                    submitted,
                    enters,
                    completed,
                    reaps,
                    sqes_per_enter = ratio(submitted, enters),
                    cqes_per_reap = ratio(completed, reaps),
                    "ring stats"
                );
            }
        }
    });
}

fn ratio(n: u64, d: u64) -> f64 {
    if d == 0 { 0.0 } else { n as f64 / d as f64 }
}
