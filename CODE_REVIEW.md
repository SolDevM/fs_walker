# Code Review — `fs_walker`

Reviewed: `src/main.rs`, `Cargo.toml`, `build.rs`, `protos/fs_walker_svc.proto`, `.gitignore`.
Status at review: compiles cleanly (`cargo check` → only an unused-import warning).

This is a working skeleton of a gRPC streaming BFS directory walker. The findings below
are ordered by impact, then by how mechanical the fix is.

## High severity — correctness & robustness

### 1. Blocking filesystem I/O on the async runtime — `main.rs:102,108`
`WalkDir::new(...)` and `std::fs::metadata(...)` are synchronous, blocking calls running
inside a `tokio::spawn`ed async task on the Tokio worker threads. A slow/network
filesystem will starve the runtime and stall *every* task. For a directory-walking tool
this is the single biggest production issue. Run the whole walk in `spawn_blocking`, or
use a dedicated worker pool (the `TODO` at `main.rs:93` is exactly right).

### 2. Symlink cycles → possible infinite traversal — `main.rs:108`
Directories are detected with `std::fs::metadata`, which **follows symlinks**. A symlink
to a directory is treated as a real directory, re-enqueued, and a cyclic symlink (pointing
to an ancestor) yields an unbounded loop. `DirEntry::file_type()` / `DirEntry::metadata()`
do *not* follow symlinks — prefer those, and make "follow symlinks" an explicit config flag
if it is ever wanted.

### 3. Panics on client disconnect, swallowed by a detached task — `main.rs:85,130-131`
`resp_tx.send(...).await.unwrap()` panics the moment the client drops the stream. That panic
happens inside the detached `tokio::spawn`, whose `JoinHandle` is discarded at `main.rs:85`,
so the panic is silently lost. Mirror what is already done at `main.rs:142` — `.is_err()` →
`break` — and never `unwrap()` on a send.

### 4. Walk errors are silently discarded — `main.rs:104`
`.filter_map(Result::ok)` hides every `walkdir::Error` (most commonly `PermissionDenied`).
The surrounding code carefully handles `metadata` errors, so this is inconsistent — a
directory that cannot be read just vanishes from the output with no signal. Iterate the
result and decide (skip-with-log, or stream an error item).

## Idiomatic Rust & succinctness

- `main.rs:2` — `use std::{thread, time};` are unused (left over from the commented `sleep`
  at `main.rs:146`). `cargo check` flags it. Delete both imports and the commented line.
- `main.rs:40-42` — `tokio::spawn(async { run_test_client().await; }).await?;` is just
  `run_test_client().await;`. The spawn adds nothing.
- `main.rs:130` — `std::format!("Err: {e}")`: `format!` (bare) is the idiom; the `std::`
  path is valid but unusual. Better still, drop it entirely — see finding #3.
- `main.rs:136` — `dir_items.len() > 0` → `!dir_items.is_empty()`.
- `main.rs:105` — `.starts_with(".")` allocates a `String` from the `Cow`; use
  `.starts_with('.')` (a `char`) and compute the `file_name` string once per entry instead
  of three times (`main.rs:105,117,123`).
- `main.rs:90,115,123` — `String::from(x.to_string_lossy())` → `x.to_string_lossy().into_owned()`
  (clearer intent; avoids a double allocation in the `String::from` case).
- `main.rs:27,34` — `.parse().unwrap()` on user input and on the addr panics on bad input.
  Replace with `clap` (below) and a `const`/`FromStr` for the address.
- `Cargo.toml:10` — `protoc = "2.28.0"` belongs in `[build-dependencies]` only if `build.rs`
  used it (it doesn't), and that crate is deprecated anyway. It is currently compiled-but-unused.
  Use `protoc-bin-vendored` in `[build-dependencies]` so builds don't require a system `protoc`.
- `main.rs:100,110-112` — the `println!` debug spew (`Walking dir`, `dirs_found: {:?}`…) is
  pure noise; see tracing below.

A tighter core loop, keeping the blocking I/O off the runtime and folding the fixes above:

```rust
fn walk(root: String, tx: mpsc::Sender<Result<FindResponse, Status>>) {
    let mut queue = VecDeque::from([(0u32, root)]);
    let mut next_id = 0u32;
    while let Some((parent_id, dir)) = queue.pop_front() {
        let mut items = Vec::new();
        for entry in WalkDir::new(&dir).max_depth(1).into_iter().filter_map(Result::ok) {
            if entry.depth() == 0 { continue; }
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with('.') { continue; }
            if entry.file_type().is_dir() {
                next_id += 1;
                queue.push_back((next_id, entry.path().to_string_lossy().into_owned()));
                items.push(FsItem { name, attr: Some(fs_item::Attr::DirId(next_id)) });
            } else {
                let size = entry.metadata().map(|m| m.len()).unwrap_or(0);
                items.push(FsItem { name, attr: Some(fs_item::Attr::FileSize(size)) });
            }
        }
        if !items.is_empty() && tx.blocking_send(Ok(FindResponse {
            fs_items: vec![FsDirContents { parent_id, items }],
        })).is_err() {
            break; // client gone
        }
    }
}

// in find_first:
tokio::task::spawn_blocking(move || walk(root, tx));
```

(`blocking_send` is the blocking-context analogue of `send`; note it needs
`Result<FindResponse, Status>` items to carry walk errors to the client.)

## Production-grade template pointers

- **CLI** — replace the positional `0|1` mode with `clap` derive subcommands
  (`server` / `client`), with `--root`, `--addr`, `--port`.
- **Logging** — swap all `println!` for `tracing` + `tracing-subscriber` (with an
  `EnvFilter`). Mandatory for a server.
- **Graceful shutdown** — `Server::builder().serve_with_shutdown(addr, async { tokio::signal::ctrl_c().await.ok(); })`
  instead of bare `.serve()`.
- **Config & safety knobs** — recursion depth limit, max items, follow-symlinks flag,
  hidden-file flag (currently `.git` and *all* dotfiles are skipped implicitly at
  `main.rs:105`, with no way to turn that off), TLS config (the `tls` feature is already
  pulled — wire it in via `tonic::transport::ServerTlsConfig`).
- **Error handling** — `anyhow`/`thiserror`; no `unwrap()` in the server path.
  `Box<dyn Error>` from `main` is acceptable for a bin, but a typed error is nicer for a
  template.
- **Cancellation** — tie the spawned walk to the request's lifetime (drop the `JoinHandle`
  on stream drop, or `select!` on the stream's cancellation) so a disconnected client stops
  the traversal.
- **Concurrency** — the `TODO` is the real feature: a worker pool (`tokio::spawn_blocking`
  + a shared queue, or `rayon`) to read subdirs in parallel; today's BFS is fully serial.
- **Build/reproducibility** — `rust-version`, `[profile.release]`
  (`lto = true`, `codegen-units = 1`, `strip = true`), commit `Cargo.lock` (already done),
  drop the stale `protoc` dep. Consider `edition = "2024"`.
- **Testing/CI** — there are zero tests. Add unit tests for the walk (using a `tempfile`
  -created tree), an integration test that drives the gRPC stream via the client, and CI
  running `cargo fmt --check`, `cargo clippy -- -D warnings`, `cargo test`.
- **Observability/ops** — `tonic-reflection` for tooling, a `/health` endpoint, and a
  `Dockerfile` since this ships as a long-running service.
- **Non-UTF-8 paths** — `to_string_lossy()` silently mangles non-UTF-8 names (proto `string`
  forces this). At minimum document the limitation; ideally note it in the response schema.

## Proto / schema

- `FindResponse.fs_items` is a `repeated FsDirContents` but exactly one is always sent —
  either flatten the schema or actually batch multiple directories per response (which pairs
  naturally with the parallel worker pool).
- `proto2` is unusual for a new service; use `proto3` unless `required` semantics are
  genuinely needed.
