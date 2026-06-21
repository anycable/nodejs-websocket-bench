// Socketioxide bench server. Mirrors the shape of backend/src/socketio/server.ts
// so the bench-runner's existing socket.io-client driver can target it
// with `?serverUrl=<this>` and no protocol-specific work needed.
//
// Endpoints:
//   GET  /health         service liveness probe
//   GET  /stats          { connections }
//   GET  /stats-csr      { connections, recovered }   (CSR mode)
//   POST /_broadcast     { stream, data } -> io.to(stream).emit("message", data)
//   POST /publish-local  in-process publish loop, mirrors the Node server's
//                        /publish-local for the "publisher inside the WS
//                        process" diagnostic test.
//
// Env:
//   PORT           default 3000
//   SOCKETIO_CSR   "1" enables Connection State Recovery
//
// FIXME(socketioxide author): the CSR feature flag and the with_state_recovery
// builder call below should be checked against the version pinned in
// Cargo.toml. The version of socketioxide we target has moved around the
// state-recovery API; this is the best-effort shape and very likely needs
// adjustment when first compiled. See the open issue for review.

use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::Json,
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use socketioxide::{extract::SocketRef, SocketIo};
use tokio::net::TcpListener;
use tracing::info;

#[derive(Clone)]
struct AppState {
    io: SocketIo,
    connections: Arc<AtomicU64>,
    recovered: Arc<AtomicU64>,
}

#[derive(Deserialize)]
struct BroadcastBody {
    stream: String,
    data: Value,
}

#[derive(Serialize)]
struct Stats {
    connections: u64,
    recovered: u64,
}

#[derive(Deserialize)]
struct PublishLocalQuery {
    total: Option<u64>,
    interval: Option<u64>,
    stream: Option<String>,
    delay: Option<u64>,
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok", "mode": "socketioxide" }))
}

async fn stats(State(state): State<AppState>) -> Json<Stats> {
    Json(Stats {
        connections: state.connections.load(Ordering::Relaxed),
        recovered: state.recovered.load(Ordering::Relaxed),
    })
}

async fn broadcast(
    State(state): State<AppState>,
    Json(body): Json<BroadcastBody>,
) -> (StatusCode, Json<Value>) {
    // The Node server accepts `data` as either a JSON object or a string
    // containing JSON (the bench-runner uses the string form). Mirror that
    // behaviour so the bench-runner's existing driver works unchanged.
    let payload = match body.data {
        Value::String(s) => serde_json::from_str::<Value>(&s).unwrap_or(Value::String(s)),
        other => other,
    };
    let _ = state.io.to(body.stream).emit("message", payload).await;
    (StatusCode::OK, Json(json!({ "ok": true })))
}

async fn publish_local(
    State(state): State<AppState>,
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
            let sent_at = chrono_millis();
            let msg = json!({ "seq": seq, "sentAt": sent_at, "text": format!("msg_{}", seq) });
            let _ = io.to(stream_for_task.clone()).emit("message", msg).await;
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

fn chrono_millis() -> i64 {
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

    let enable_csr = env::var("SOCKETIO_CSR")
        .map(|v| v == "1")
        .unwrap_or(false);

    // FIXME(socketioxide author): adjust the builder call below to enable
    // Connection State Recovery in the exact way the pinned crate version
    // expects. The shape here is illustrative.
    let builder = SocketIo::builder();
    // if enable_csr {
    //     builder = builder.with_state_recovery(
    //         socketioxide::state_recovery::StateRecoveryConfig::default(),
    //     );
    // }
    let (layer, io) = builder.build_layer();

    let state = AppState {
        io: io.clone(),
        connections: Arc::new(AtomicU64::new(0)),
        recovered: Arc::new(AtomicU64::new(0)),
    };

    let conns = state.connections.clone();
    let recovered = state.recovered.clone();
    io.ns("/", move |socket: SocketRef| {
        conns.fetch_add(1, Ordering::Relaxed);
        // FIXME(socketioxide author): when CSR is enabled, the recovered
        // flag on the socket should be checked here and `recovered`
        // incremented. The exact accessor varies by version.

        socket.on("join", |socket: SocketRef, room: Value| async move {
            if let Some(name) = room.as_str() {
                let _ = socket.join(name.to_string());
            }
        });

        socket.on("whisper", |socket: SocketRef, data: Value| async move {
            if let (Some(room), Some(payload)) =
                (data.get(0).and_then(|v| v.as_str()), data.get(1))
            {
                let _ = socket
                    .to(room.to_string())
                    .emit("whisper", payload.clone())
                    .await;
            }
        });

        let conn_dec = conns.clone();
        socket.on_disconnect(move || {
            conn_dec.fetch_sub(1, Ordering::Relaxed);
        });
    });

    let app = Router::new()
        .route("/health", get(health))
        .route("/stats", get(stats))
        .route("/stats-csr", get(stats))
        .route("/_broadcast", post(broadcast))
        .route("/publish-local", post(publish_local))
        .layer(layer)
        .with_state(state);

    let addr = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(&addr).await.expect("bind");
    info!(addr = %addr, csr = enable_csr, "socketioxide bench server listening");
    axum::serve(listener, app).await.expect("serve");
}
