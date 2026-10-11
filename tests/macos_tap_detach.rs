//! Privileged macOS regression tests for the async TAP (feth/BPF) device.
//!
//! XNU raises no poll event on the BPF descriptor when its feth interface is
//! destroyed, so a parked `recv` used to hang forever. Polling the BPF
//! descriptor for writability instead of the AF_NDRV socket used to fail with
//! POLLNVAL (`fd error`). These tests pin both behaviors, plus the properties
//! a fix must keep: no busy loop, no spurious errors on a healthy device, and
//! no change for utun devices.
//!
//! The tests drive the futures with a minimal executor and plain threads, so
//! they run unchanged with either the `async_tokio` or the `async_io`
//! feature. Every test body runs on a worker thread under a deadline, and every
//! wait inside it has its own guard, so a regression fails in bounded time
//! instead of stalling the job.
//!
//! Run with root on macOS:
//!   sudo -E cargo test --test macos_tap_detach --features async_tokio -- --ignored
#![cfg(all(
    target_os = "macos",
    any(feature = "async_tokio", feature = "async_io")
))]

use futures::executor::block_on;
use futures::FutureExt;
use std::io;
use std::net::UdpSocket;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};
use tun_rs::{AsyncDevice, DeviceBuilder, Layer};

/// Upper bound for any single wait; a regression must fail within this.
const HANG_GUARD: Duration = Duration::from_secs(10);
/// Generous bound for "woken after the feth pair was destroyed". The probe
/// interval is half a second, so this only trips on a real hang.
const WAKE_BOUND: Duration = Duration::from_secs(4);
/// Idle time that spans several probe intervals.
const PROBE_SPAN: Duration = Duration::from_millis(1500);
/// Deadline for a whole test body, setup and cleanup included.
const TEST_DEADLINE: Duration = Duration::from_secs(45);
/// Last resort: the process exits if a test (or its cleanup) stalls this long.
const WATCHDOG: Duration = Duration::from_secs(90);
/// Bound for one `ifconfig destroy`.
const CLEANUP_BOUND: Duration = Duration::from_secs(10);

/// Held by every test so they run one at a time.
static SERIAL: Mutex<()> = Mutex::new(());
/// Index of the next feth pair, keeping interface names unique per test.
static NEXT_PAIR: AtomicU32 = AtomicU32::new(0);

/// Serializes the tests; a poisoned lock from a failed test is ignored.
fn serial() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

/// Runs `ifconfig <name> destroy`, killing it if the kernel stalls so that
/// cleanup can never hang a test.
fn destroy(name: &str) {
    let Ok(mut child) = Command::new("ifconfig")
        .args([name, "destroy"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    else {
        return;
    };
    let end = Instant::now() + CLEANUP_BOUND;
    loop {
        match child.try_wait() {
            Ok(None) if Instant::now() < end => thread::sleep(Duration::from_millis(50)),
            Ok(None) => {
                eprintln!("ifconfig {name} destroy stalled; killing it");
                let _ = child.kill();
                return;
            }
            _ => return,
        }
    }
}

/// Prints the stacks of every thread of this process (best effort) so a hang
/// shows where the threads are blocked.
fn dump_threads(name: &str) {
    let out = std::env::temp_dir().join(format!("tun_rs_sample_{}.txt", std::process::id()));
    let Ok(mut child) = Command::new("sample")
        .arg(std::process::id().to_string())
        .arg("1")
        .arg("-file")
        .arg(&out)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    else {
        return;
    };
    let end = Instant::now() + Duration::from_secs(20);
    while matches!(child.try_wait(), Ok(None)) {
        if Instant::now() >= end {
            let _ = child.kill();
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }
    if let Ok(text) = std::fs::read_to_string(&out) {
        eprintln!("---- thread dump for {name} ----");
        eprintln!("{}", &text[..text.len().min(60_000)]);
    }
}

/// Runs a test body on a worker thread with a deadline. On timeout it dumps
/// the thread stacks, destroys the feth pair and fails from the test thread;
/// a watchdog ends the process if even that stalls. Tests are serialized so
/// CPU and timing measurements are not disturbed by neighbors.
fn run(name: &'static str, body: impl FnOnce(Arc<Pair>) + Send + 'static) {
    // The watchdog starts first so even a stall in the lock, in interface
    // creation or in cleanup ends the process with the test name logged.
    let finished = Arc::new(AtomicBool::new(false));
    {
        let finished = finished.clone();
        thread::spawn(move || {
            let end = Instant::now() + WATCHDOG;
            while Instant::now() < end {
                if finished.load(Ordering::SeqCst) {
                    return;
                }
                thread::sleep(Duration::from_millis(200));
            }
            // Nothing here may take a lock or allocate: write straight to
            // fd 2, then end the process without running exit handlers.
            let msg = format!("watchdog: {name} stalled past {WATCHDOG:?}; killing\n");
            unsafe {
                libc::write(2, msg.as_ptr().cast(), msg.len());
                libc::kill(libc::getpid(), libc::SIGKILL);
                libc::_exit(2);
            }
        });
    }
    let _serial = serial();
    let pair = Arc::new(Pair::new());
    let (tx, rx) = mpsc::channel();
    {
        let pair = pair.clone();
        thread::Builder::new()
            .name(name.to_string())
            .spawn(move || {
                let outcome = catch_unwind(AssertUnwindSafe(|| body(pair)));
                let _ = tx.send(outcome);
            })
            .expect("spawn test worker");
    }
    let outcome = rx.recv_timeout(TEST_DEADLINE);
    if outcome.is_err() {
        dump_threads(name);
    }
    pair.destroy();
    finished.store(true, Ordering::SeqCst);
    match outcome {
        Ok(Ok(())) => {}
        Ok(Err(payload)) => resume_unwind(payload),
        Err(_) => panic!("{name}: did not finish within {TEST_DEADLINE:?}"),
    }
}

/// A uniquely named feth pair, destroyed on every exit path.
struct Pair {
    /// Unique pair index, also used to derive addresses.
    idx: u32,
    /// Name of the TAP-side interface.
    dev: String,
    /// Name of the peer interface the BPF descriptor binds to.
    peer: String,
    /// Set once the pair was destroyed, so the kernel is never asked to
    /// destroy an interface that is still detaching from an earlier request.
    destroyed: AtomicBool,
}

impl Pair {
    /// Reserves the next free pair name and removes any leftover interfaces.
    fn new() -> Self {
        let idx = NEXT_PAIR.fetch_add(1, Ordering::SeqCst);
        let base = 60 + 2 * idx;
        let pair = Pair {
            idx,
            dev: format!("feth{base}"),
            peer: format!("feth{}", base + 1),
            destroyed: AtomicBool::new(false),
        };
        destroy(&pair.dev);
        destroy(&pair.peer);
        pair
    }

    /// Destroys both interfaces of the pair; only the first call acts.
    fn destroy(&self) {
        if self.destroyed.swap(true, Ordering::SeqCst) {
            return;
        }
        destroy(&self.dev);
        destroy(&self.peer);
    }

    /// Address of the TAP-side interface, unique per pair.
    fn addr(&self, host: u8) -> String {
        format!("10.90.{}.{host}", self.idx)
    }

    /// Builder for an L2 device on `dev` with `peer` as the feth peer.
    fn builder(&self) -> DeviceBuilder {
        DeviceBuilder::new()
            .name(self.dev.clone())
            .layer(Layer::L2)
            .peer_feth(self.peer.clone())
    }

    /// Creates the async TAP without an IP address.
    fn build(&self) -> Arc<AsyncDevice> {
        Arc::new(
            self.builder()
                .build_async()
                .expect("create async TAP (needs root)"),
        )
    }

    /// Creates the async TAP with `addr(1)/24`, so it emits ARP traffic.
    fn build_with_ip(&self) -> Arc<AsyncDevice> {
        Arc::new(
            self.builder()
                .ipv4(self.addr(1), 24, None)
                .build_async()
                .expect("create async TAP with address (needs root)"),
        )
    }
}

impl Drop for Pair {
    /// Destroys the interfaces so no test leaves a feth behind.
    fn drop(&mut self) {
        self.destroy();
    }
}

/// `true` for the code-less `fd error` the readiness wait used to return.
fn is_fd_error(e: &io::Error) -> bool {
    e.raw_os_error().is_none() && e.to_string() == "fd error"
}

/// Asserts that `e` carries the kernel's `ENXIO` from a detached interface.
fn assert_enxio(e: &io::Error, ctx: &str) {
    assert_eq!(e.raw_os_error(), Some(libc::ENXIO), "{ctx}: {e:?}");
}

/// Asserts that `e` is a real OS error: not a timeout and not `fd error`.
fn assert_os_error(e: &io::Error, ctx: &str) {
    assert!(
        e.raw_os_error().is_some() && e.kind() != io::ErrorKind::TimedOut && !is_fd_error(e),
        "{ctx}: expected an OS error code, got {e:?}"
    );
}

/// Receives until the device reports an error (frames that were already
/// queued are drained first) and returns that error.
fn recv_until_err(dev: &AsyncDevice) -> io::Error {
    let mut buf = vec![0u8; 9216];
    loop {
        if let Err(e) = block_on(dev.recv(&mut buf)) {
            return e;
        }
    }
}

/// Starts a thread that receives until the first error and reports it.
fn spawn_recv(dev: &Arc<AsyncDevice>) -> Receiver<io::Error> {
    let (tx, rx) = mpsc::channel();
    let dev = dev.clone();
    thread::spawn(move || {
        let _ = tx.send(recv_until_err(&dev));
    });
    rx
}

/// Waits up to `HANG_GUARD` for the reader's error; fails if it never wakes.
fn expect_err(rx: &Receiver<io::Error>, ctx: &str) -> io::Error {
    match rx.recv_timeout(HANG_GUARD) {
        Ok(e) => e,
        Err(RecvTimeoutError::Timeout) => panic!("{ctx}: recv hung (no wake-up)"),
        Err(RecvTimeoutError::Disconnected) => panic!("{ctx}: reader thread died"),
    }
}

/// Runs `f` on a helper thread and fails if it does not finish in time.
fn guarded<T: Send + 'static>(ctx: &str, f: impl FnOnce() -> T + Send + 'static) -> T {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let _ = tx.send(f());
    });
    match rx.recv_timeout(HANG_GUARD) {
        Ok(v) => v,
        Err(RecvTimeoutError::Timeout) => {
            panic!("{ctx}: did not finish within {HANG_GUARD:?}")
        }
        Err(RecvTimeoutError::Disconnected) => panic!("{ctx}: the helper thread panicked"),
    }
}

/// A 64-byte broadcast Ethernet frame with a local-experimental ethertype.
fn test_frame() -> Vec<u8> {
    let mut frame = vec![0u8; 64];
    frame[..6].copy_from_slice(&[0xff; 6]);
    frame[6..12].copy_from_slice(&[0x02, 0x00, 0x00, 0x00, 0x00, 0x01]);
    frame[12..14].copy_from_slice(&0x88b5u16.to_be_bytes());
    frame
}

/// User plus system CPU time consumed by this process so far.
fn cpu_time() -> Duration {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    assert_eq!(rc, 0, "getrusage failed");
    let tv = |t: libc::timeval| Duration::new(t.tv_sec as u64, (t.tv_usec as u32) * 1000);
    tv(ru.ru_utime) + tv(ru.ru_stime)
}

/// Provokes ARP requests out of the TAP-side interface.
fn poke_neighbor(pair: &Pair) {
    if let Ok(sock) = UdpSocket::bind("0.0.0.0:0") {
        let _ = sock.send_to(b"x", format!("{}:9", pair.addr(2)));
    }
}

// ---------------------------------------------------------------------------
// Detach wake-up
// ---------------------------------------------------------------------------

/// A recv parked in the readiness wait fails with `ENXIO` after the destroy.
#[ignore = "requires root and creates feth interfaces"]
#[test]
fn pending_recv_errors_after_feth_pair_destroyed() {
    run("pending_recv_errors_after_feth_pair_destroyed", |pair| {
        let dev = pair.build();
        let rx = spawn_recv(&dev);
        // Let the reader park in the readiness wait, then destroy the pair.
        thread::sleep(Duration::from_millis(500));
        pair.destroy();
        let start = Instant::now();
        let err = expect_err(&rx, "destroy while recv parked");
        assert!(
            start.elapsed() < WAKE_BOUND,
            "wake took {:?}",
            start.elapsed()
        );
        assert_enxio(&err, "destroy while recv parked");
    });
}

/// A first recv issued after the destroy fails with `ENXIO` without hanging.
#[ignore = "requires root and creates feth interfaces"]
#[test]
fn recv_after_destroy_before_first_recv_errors_immediately() {
    run(
        "recv_after_destroy_before_first_recv_errors_immediately",
        |pair| {
            let dev = pair.build();
            pair.destroy();
            let start = Instant::now();
            let rx = spawn_recv(&dev);
            let err = expect_err(&rx, "destroy before first recv");
            assert!(start.elapsed() < WAKE_BOUND, "took {:?}", start.elapsed());
            assert_enxio(&err, "destroy before first recv");
        },
    );
}

/// Both of two concurrently parked recvs wake with `ENXIO`.
#[ignore = "requires root and creates feth interfaces"]
#[test]
fn two_parked_recvs_all_wake() {
    parked_recvs_all_wake("two_parked_recvs_all_wake", 2);
}

/// Every one of many concurrently parked recvs wakes with `ENXIO`.
///
/// Opt-in stress test, skipped by the CI script: with four or more parked
/// readers on a GitHub-hosted macOS runner the readers were not woken and the
/// test process then stayed in the kernel's exit state, which `kill -9` cannot
/// remove and which can freeze the whole runner. Run it by hand, never in a
/// shared environment. The reader count comes from `TAP_DETACH_PARKED`
/// (default 16).
#[ignore = "hazardous stress test: can wedge a macOS kernel; requires root"]
#[test]
fn many_parked_recvs_all_wake_stress() {
    let parked: usize = std::env::var("TAP_DETACH_PARKED")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|n| *n > 0)
        .unwrap_or(16);
    parked_recvs_all_wake("many_parked_recvs_all_wake_stress", parked);
}

/// Unix time in milliseconds, for correlating with the library's `tap-diag`
/// lines.
fn unix_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

/// Diagnostic: stderr line stamped with unix milliseconds.
fn dlog(msg: &str) {
    use std::io::Write;
    let line = format!("[test-diag unix_ms={}] {msg}\n", unix_ms());
    unsafe {
        libc::write(2, line.as_ptr().cast(), line.len());
    }
    if let Some(path) = std::env::var_os("TAP_DIAG_FILE") {
        if let Ok(mut file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
        {
            let _ = file.write_all(line.as_bytes());
        }
    }
}

/// Diagnostic: `if_nametoindex` of `name` (`0` when it does not exist).
fn nametoindex(name: &str) -> u32 {
    let c = std::ffi::CString::new(name).unwrap();
    unsafe { libc::if_nametoindex(c.as_ptr()) }
}

/// Diagnostic: the interface names listed by `ifconfig -l`.
fn ifconfig_list() -> Vec<String> {
    Command::new("ifconfig")
        .arg("-l")
        .stderr(Stdio::null())
        .output()
        .map(|o| {
            String::from_utf8_lossy(&o.stdout)
                .split_whitespace()
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

/// Diagnostic, not a regression test: parks `TAP_DETACH_PARKED` (default 4)
/// readers on one shared TAP device, destroys the feth pair, then for 8
/// seconds logs once per second the interface indexes, the `ifconfig -l`
/// listing and how many readers have returned. Ends the process with
/// `_exit(0)` so thread teardown cannot wedge the test. Run with `TAP_DIAG=1`
/// to also get the library's per-thread wait/probe lines. `TAP_DIAG_MODE` is
/// `shared` (the only implemented mode: all readers use one device handle).
#[ignore = "diagnostic: requires root, creates feth interfaces, exits the process"]
#[test]
fn diag_parked_readers() {
    let parked: usize = std::env::var("TAP_DETACH_PARKED")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|n| *n > 0)
        .unwrap_or(4);
    let mode = std::env::var("TAP_DIAG_MODE").unwrap_or_else(|_| "shared".into());
    run("diag_parked_readers", move |pair| {
        dlog(&format!("mode={mode} parked={parked}"));
        if mode != "shared" {
            dlog("only the shared mode is implemented; running shared");
        }
        let dev = pair.build();
        let returned = Arc::new(AtomicU32::new(0));
        let results = Arc::new(Mutex::new(Vec::<String>::new()));
        for i in 0..parked {
            let dev = dev.clone();
            let returned = returned.clone();
            let results = results.clone();
            thread::spawn(move || {
                let err = recv_until_err(&dev);
                let line = format!("reader {i} returned {err:?} at unix_ms={}", unix_ms());
                dlog(&line);
                results.lock().unwrap_or_else(|e| e.into_inner()).push(line);
                returned.fetch_add(1, Ordering::SeqCst);
            });
        }
        thread::sleep(Duration::from_millis(500));
        dlog(&format!(
            "before destroy: {}={} {}={} listed={:?}",
            pair.dev,
            nametoindex(&pair.dev),
            pair.peer,
            nametoindex(&pair.peer),
            ifconfig_list()
                .into_iter()
                .filter(|n| n.starts_with("feth"))
                .collect::<Vec<_>>()
        ));
        dlog("destroy start");
        pair.destroy();
        dlog("destroy returned");
        for sec in 1..=8 {
            thread::sleep(Duration::from_secs(1));
            // The sample runs on a helper so a stalled kernel call is visible
            // as a missing line instead of freezing the loop.
            let (dev_name, peer_name) = (pair.dev.clone(), pair.peer.clone());
            let (tx, rx) = mpsc::channel();
            thread::spawn(move || {
                let listed = ifconfig_list();
                let _ = tx.send((
                    nametoindex(&dev_name),
                    nametoindex(&peer_name),
                    listed.contains(&dev_name),
                    listed.contains(&peer_name),
                ));
            });
            let woke = returned.load(Ordering::SeqCst);
            match rx.recv_timeout(Duration::from_millis(800)) {
                Ok((di, pi, dl, pl)) => dlog(&format!(
                    "t+{sec}s: main {}: index={di} listed={dl}; peer {}: index={pi} listed={pl}; readers returned {woke}/{parked}",
                    pair.dev, pair.peer
                )),
                Err(_) => dlog(&format!(
                    "t+{sec}s: interface sample stalled; readers returned {woke}/{parked}"
                )),
            }
        }
        let woke = returned.load(Ordering::SeqCst);
        dlog(&format!("RESULT: {woke} of {parked} readers returned"));
        for line in results.lock().unwrap_or_else(|e| e.into_inner()).iter() {
            dlog(line);
        }
        dlog("calling _exit(0)");
        unsafe { libc::_exit(0) };
    });
}

/// Parks `parked` recvs on one TAP device, destroys its feth pair and checks
/// that every reader wakes with `ENXIO`.
fn parked_recvs_all_wake(name: &'static str, parked: usize) {
    run(name, move |pair| {
        let dev = pair.build();
        let readers: Vec<_> = (0..parked).map(|_| spawn_recv(&dev)).collect();
        thread::sleep(Duration::from_millis(500));
        pair.destroy();
        // One shared deadline; report how many readers woke on failure.
        let deadline = Instant::now() + HANG_GUARD;
        let mut woke = 0;
        let mut errors = Vec::new();
        for rx in &readers {
            let left = deadline.saturating_duration_since(Instant::now());
            if let Ok(e) = rx.recv_timeout(left) {
                woke += 1;
                errors.push(e);
            }
        }
        assert_eq!(
            woke,
            readers.len(),
            "only {woke} of {} parked readers woke after the destroy",
            readers.len()
        );
        for (i, e) in errors.iter().enumerate() {
            assert_enxio(e, &format!("parked reader {i}"));
        }
    });
}

/// `readable()` resolves after the destroy and `try_recv` then returns `ENXIO`.
#[ignore = "requires root and creates feth interfaces"]
#[test]
fn readable_then_try_recv_surfaces_the_error() {
    run("readable_then_try_recv_surfaces_the_error", |pair| {
        let dev = pair.build();
        let (tx, rx) = mpsc::channel();
        {
            let dev = dev.clone();
            thread::spawn(move || {
                let mut buf = vec![0u8; 9216];
                let err = loop {
                    if let Err(e) = block_on(dev.readable()) {
                        break e;
                    }
                    match dev.try_recv(&mut buf) {
                        Ok(_) => {}
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                        Err(e) => break e,
                    }
                };
                let _ = tx.send(err);
            });
        }
        thread::sleep(Duration::from_millis(500));
        pair.destroy();
        let err = expect_err(&rx, "readable + try_recv");
        assert_enxio(&err, "readable + try_recv");
    });
}

/// The poll-based `poll_recv` API also wakes with `ENXIO` after the destroy.
#[ignore = "requires root and creates feth interfaces"]
#[test]
fn poll_recv_wakes_after_destroy() {
    run("poll_recv_wakes_after_destroy", |pair| {
        let dev = pair.build();
        let (tx, rx) = mpsc::channel();
        {
            let dev = dev.clone();
            thread::spawn(move || {
                let mut buf = vec![0u8; 9216];
                let err = loop {
                    match block_on(futures::future::poll_fn(|cx| dev.poll_recv(cx, &mut buf))) {
                        Ok(_) => {}
                        Err(e) => break e,
                    }
                };
                let _ = tx.send(err);
            });
        }
        thread::sleep(Duration::from_millis(500));
        pair.destroy();
        let err = expect_err(&rx, "poll_recv");
        assert_enxio(&err, "poll_recv");
    });
}

/// Repeated recv and readable calls on a detached device keep failing with an
/// OS error, finish promptly and do not burn CPU.
#[ignore = "requires root and creates feth interfaces"]
#[test]
fn recv_keeps_erroring_after_error_without_spinning() {
    run("recv_keeps_erroring_after_error_without_spinning", |pair| {
        let dev = pair.build();
        let rx = spawn_recv(&dev);
        thread::sleep(Duration::from_millis(300));
        pair.destroy();
        assert_enxio(&expect_err(&rx, "first error"), "first error");

        let started = cpu_time();
        let wall = Instant::now();
        let dev2 = dev.clone();
        guarded("repeated recv/readable after error", move || {
            let mut buf = vec![0u8; 9216];
            for i in 0..100 {
                let err = block_on(dev2.recv(&mut buf)).expect_err("recv must keep failing");
                assert_os_error(&err, &format!("repeat {i}"));
                // readable() either reports the error or resolves so the next
                // read can; it must never park forever.
                if let Err(e) = block_on(dev2.readable()) {
                    assert_os_error(&e, &format!("readable repeat {i}"));
                }
            }
        });
        assert!(wall.elapsed() < HANG_GUARD);
        assert!(
            cpu_time() - started < Duration::from_secs(5),
            "erroring loop burned {:?} of CPU",
            cpu_time() - started
        );
    });
}

/// Cancelling parked recvs leaves the wait machinery intact for the next
/// reader, which still wakes with `ENXIO`.
#[ignore = "requires root and creates feth interfaces"]
#[test]
fn dropped_parked_recv_does_not_break_the_next_wake() {
    run("dropped_parked_recv_does_not_break_the_next_wake", |pair| {
        let dev = pair.build();
        let mut buf = vec![0u8; 9216];
        for _ in 0..20 {
            // Poll a recv until it parks (a stray frame may complete it first),
            // then drop it to exercise the cancel path.
            for _ in 0..20 {
                if dev.recv(&mut buf).now_or_never().is_none() {
                    break;
                }
            }
        }
        let rx = spawn_recv(&dev);
        thread::sleep(Duration::from_millis(500));
        pair.destroy();
        let err = expect_err(&rx, "recv after cancellations");
        assert_enxio(&err, "recv after cancellations");
    });
}

/// Frames queued before the destroy are drained, then recv ends in `ENXIO`.
#[ignore = "requires root and creates feth interfaces"]
#[test]
fn queued_frames_then_destroy_end_in_error() {
    run("queued_frames_then_destroy_end_in_error", |pair| {
        let dev = pair.build_with_ip();
        for _ in 0..5 {
            poke_neighbor(&pair);
            thread::sleep(Duration::from_millis(100));
        }
        pair.destroy();
        let rx = spawn_recv(&dev);
        let err = expect_err(&rx, "queued frames then destroy");
        assert_enxio(&err, "queued frames then destroy");
    });
}

/// `send` on a detached device fails with an OS error, and `writable()` never
/// returns the code-less `fd error`.
#[ignore = "requires root and creates feth interfaces"]
#[test]
fn send_after_destroy_reports_an_os_error() {
    run("send_after_destroy_reports_an_os_error", |pair| {
        let dev = pair.build();
        pair.destroy();
        let frame = test_frame();
        let dev2 = dev.clone();
        let result = guarded("send after destroy", move || block_on(dev2.send(&frame)));
        let err = result.expect_err("send on a destroyed feth must fail");
        assert_os_error(&err, "send after destroy");

        let dev3 = dev.clone();
        let writable = guarded("writable after destroy", move || block_on(dev3.writable()));
        if let Err(e) = writable {
            assert!(!is_fd_error(&e), "writable after destroy: {e:?}");
        }
    });
}

// ---------------------------------------------------------------------------
// Healthy device
// ---------------------------------------------------------------------------

/// A live TAP idle across several probe intervals is not reported as
/// detached and still receives frames afterwards.
#[ignore = "requires root and creates feth interfaces"]
#[test]
fn healthy_tap_keeps_receiving_across_the_probe_interval() {
    run(
        "healthy_tap_keeps_receiving_across_the_probe_interval",
        |pair| {
            let dev = pair.build_with_ip();
            let (tx, rx) = mpsc::channel::<io::Result<Vec<u8>>>();
            {
                let dev = dev.clone();
                thread::spawn(move || {
                    let mut buf = vec![0u8; 9216];
                    loop {
                        match block_on(dev.recv(&mut buf)) {
                            Ok(n) => {
                                if tx.send(Ok(buf[..n].to_vec())).is_err() {
                                    return;
                                }
                            }
                            Err(e) => {
                                let _ = tx.send(Err(e));
                                return;
                            }
                        }
                    }
                });
            }
            // Idle across several probe intervals: the probe must not report a live
            // device as detached.
            thread::sleep(PROBE_SPAN);
            let target = pair.addr(2).parse::<std::net::Ipv4Addr>().unwrap().octets();
            let deadline = Instant::now() + HANG_GUARD;
            let mut saw_arp = false;
            while !saw_arp && Instant::now() < deadline {
                poke_neighbor(&pair);
                loop {
                    match rx.recv_timeout(Duration::from_millis(250)) {
                        Ok(Ok(frame)) => {
                            if frame.len() >= 42
                                && frame[12..14] == [0x08, 0x06]
                                && frame[38..42] == target
                            {
                                saw_arp = true;
                                break;
                            }
                        }
                        Ok(Err(e)) => panic!("live TAP reported an error: {e:?}"),
                        Err(_) => break,
                    }
                }
            }
            assert!(saw_arp, "no ARP request for {target:?} reached the reader");
        },
    );
}

/// `writable()` resolves on a live TAP (polled on the AF_NDRV socket) while a
/// reader stays parked.
#[ignore = "requires root and creates feth interfaces"]
#[test]
fn writable_resolves_on_a_live_tap() {
    run("writable_resolves_on_a_live_tap", |pair| {
        let dev = pair.build();
        // A parked reader must not interfere with writability waits.
        let reader = spawn_recv(&dev);
        thread::sleep(Duration::from_millis(300));
        let dev2 = dev.clone();
        guarded("writable on a live TAP", move || {
            for i in 0..10 {
                match block_on(dev2.writable()) {
                    Ok(()) => {}
                    Err(e) => panic!("writable() #{i} failed on a live TAP: {e:?}"),
                }
            }
            let n = block_on(dev2.send(&test_frame())).expect("send on a live TAP");
            assert!(n > 0);
        });
        assert!(
            reader.try_recv().is_err(),
            "reader must still be parked on a live TAP"
        );
    });
}

/// An idle parked recv on a live TAP uses little CPU and stays pending.
#[ignore = "requires root and creates feth interfaces"]
#[test]
fn idle_recv_on_a_live_tap_does_not_spin() {
    run("idle_recv_on_a_live_tap_does_not_spin", |pair| {
        let dev = pair.build();
        let reader = spawn_recv(&dev);
        thread::sleep(Duration::from_millis(300));
        let before = cpu_time();
        thread::sleep(Duration::from_secs(2));
        let used = cpu_time() - before;
        assert!(
            used < Duration::from_secs(1),
            "idle TAP wait used {used:?} CPU in 2 s (busy loop?)"
        );
        assert!(reader.try_recv().is_err(), "idle reader must stay parked");
    });
}

/// An idle utun recv never errors: the detach probe applies to TAP only.
#[ignore = "requires root and creates utun interfaces"]
#[test]
fn utun_idle_recv_is_not_woken_by_the_probe() {
    run("utun_idle_recv_is_not_woken_by_the_probe", |_pair| {
        // The tokio backend registers the utun fd with the reactor on
        // creation, which needs a runtime context.
        #[cfg(feature = "async_tokio")]
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("tokio runtime");
        #[cfg(feature = "async_tokio")]
        let _entered = runtime.enter();
        let dev = Arc::new(
            DeviceBuilder::new()
                .ipv4("10.91.0.1", 24, None)
                .build_async()
                .expect("create async TUN (needs root)"),
        );
        let (tx, rx) = mpsc::channel::<io::Result<usize>>();
        {
            let dev = dev.clone();
            thread::spawn(move || {
                let mut buf = vec![0u8; 9216];
                loop {
                    let r = block_on(dev.recv(&mut buf));
                    let stop = r.is_err();
                    if tx.send(r).is_err() || stop {
                        return;
                    }
                }
            });
        }
        // Stray packets are fine; an error from an idle utun is not.
        let end = Instant::now() + PROBE_SPAN;
        while let Some(left) = end.checked_duration_since(Instant::now()) {
            match rx.recv_timeout(left) {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => panic!("idle utun recv failed: {e:?}"),
                Err(_) => break,
            }
        }
    });
}
