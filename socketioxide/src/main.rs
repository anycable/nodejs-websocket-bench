// Socketioxide bench server. Mirrors the shape of backend/src/socketio/server.ts
// so the bench-runner's existing socket.io-client driver can target it
// with `?serverUrl=<this>` and no protocol-specific work needed.
//
// Endpoints:
//   GET  /health         service liveness probe
//   GET  /stats          { connections }
//   POST /_broadcast     { stream, data } -> io.to(stream).emit("message", data)
//   POST /publish-local  in-process publish loop, mirrors the Node server's
//                        /publish-local for the "publisher inside the WS
//                        process" diagnostic test.
//
// Env:
//   PORT           default 3000
//
// CSR (Connection State Recovery) is intentionally not implemented here.
// socketioxide 0.18 doesn't ship server-side session resume (no CSR-shaped
// feature flag, no mention in README or examples). If the library adds it,
// we'll wire it the same way the Node server does. See
// docs/socketioxide-comparison.md for the open question to the library author.

use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::{Query, State as AxumState},
    http::StatusCode,
    response::Json,
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use socketioxide::{
    extract::{Data, SocketRef, State as IoState},
    SocketIo,
};
use tokio::net::TcpListener;
use tracing::info;

// Shared connection counter. Registered as socketioxide state via
// `.with_state(Arc<AtomicU64>)` (read in handlers through the State
// extractor) and cloned into the axum router state so GET /stats reads
// the same Arc.
type ConnCounter = Arc<AtomicU64>;

// Router state for the HTTP endpoints: the io handle (to broadcast) plus
// the same counter Arc.
#[derive(Clone)]
struct HttpState {
    io: SocketIo,
    connections: ConnCounter,
}

#[derive(Deserialize)]
struct BroadcastBody {
    stream: String,
    data: Value,
}

#[derive(Serialize)]
struct Stats {
    connections: u64,
}

#[derive(Deserialize)]
struct PublishLocalQuery {
    total: Option<u64>,
    interval: Option<u64>,
    stream: Option<String>,
    delay: Option<u64>,
}

// --- socketioxide handlers ---------------------------------------------------

async fn on_connect(s: SocketRef, connections: IoState<ConnCounter>) {
    connections.fetch_add(1, Ordering::Relaxed);

    // Client emits `join` with the stream name (a string).
    s.on("join", |s: SocketRef, Data::<String>(room)| async move {
        let _ = s.join(room);
    });

    // Whisper: client emits ("whisper", room, payload). socketioxide
    // delivers multiple emit args as a tuple. Forward to everyone else
    // in the room. Out of scope for the jitter/latency/idle/avalanche
    // tests but kept for parity with the Node server.
    s.on(
        "whisper",
        |s: SocketRef, Data::<(String, Value)>((room, payload))| async move {
            let _ = s.to(room).emit("whisper", &payload).await;
        },
    );

    s.on_disconnect(on_disconnect);
}

async fn on_disconnect(connections: IoState<ConnCounter>) {
    connections.fetch_sub(1, Ordering::Relaxed);
}

// --- HTTP handlers -----------------------------------------------------------

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok", "mode": "socketioxide" }))
}

async fn stats(AxumState(state): AxumState<HttpState>) -> Json<Stats> {
    Json(Stats {
        connections: state.connections.load(Ordering::Relaxed),
    })
}

async fn broadcast(
    AxumState(state): AxumState<HttpState>,
    Json(body): Json<BroadcastBody>,
) -> (StatusCode, Json<Value>) {
    // The Node server accepts `data` as either a JSON object or a string
    // containing JSON (the bench-runner sends the string form). Mirror that
    // so the bench-runner's existing driver works unchanged.
    let payload = match body.data {
        Value::String(s) => serde_json::from_str::<Value>(&s).unwrap_or(Value::String(s)),
        other => other,
    };
    let _ = state.io.to(body.stream).emit("message", &payload).await;
    (StatusCode::OK, Json(json!({ "ok": true })))
}

async fn publish_local(
    AxumState(state): AxumState<HttpState>,
    Query(q): Query<PublishLocalQuery>,
) -> Json<Value> {
    let total = q.total.unwrap_or(120);
    let interval = q.interval.unwrap_or(500);
    let stream = q.stream.unwrap_or_else(|| "benchmark".to_string());
    let delay = q.delay.unwrap_or(0);

    let io = state.io.clone();
    let stream_for_task = stream.clone();
    tokio::spawn(async move {
        if delay > 0 {
            tokio::time::sleep(Duration::from_secs(delay)).await;
        }
        for seq in 1..=total {
            let sent_at = unix_millis();
            let msg = json!({ "seq": seq, "sentAt": sent_at, "text": format!("msg_{}", seq) });
            let _ = io.to(stream_for_task.clone()).emit("message", &msg).await;
            tokio::time::sleep(Duration::from_millis(interval)).await;
        }
    });

    Json(json!({
        "status": "publishing-local",
        "total": total,
        "interval": interval,
        "stream": stream,
        "delay": delay,
    }))
}

fn unix_millis() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let port: u16 = env::var("PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3000);

    let connections: ConnCounter = Arc::new(AtomicU64::new(0));

    let (layer, io) = SocketIo::builder()
        .with_state(connections.clone())
        .build_layer();

    io.ns("/", on_connect);

    let http_state = HttpState {
        io: io.clone(),
        connections: connections.clone(),
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/stats", get(stats))
        .route("/_broadcast", post(broadcast))
        .route("/publish-local", post(publish_local))
        .layer(layer)
        .with_state(http_state);

    // Bind IPv6 any (`[::]`), which is dual-stack on Linux and, crucially,
    // is what Railway's private network (*.railway.internal) routes over.
    // Binding 0.0.0.0 (IPv4-only) makes the service unreachable internally.
    let addr = format!("[::]:{}", port);
    let listener = TcpListener::bind(&addr).await.expect("bind");
    info!(addr = %addr, "socketioxide bench server listening");
    axum::serve(listener, app).await.expect("serve");
}
