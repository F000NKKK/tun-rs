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
//! feature. Every wait is behind a hang guard, so a regression fails fast
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
use std::process::Command;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};
use tun_rs::{AsyncDevice, DeviceBuilder, Layer};

/// Upper bound for any single wait; a regression must fail within this.
const HANG_GUARD: Duration = Duration::from_secs(10);
/// Generous bound for "woken after the feth pair was destroyed". The probe
/// interval is a quarter of a second, so this only trips on a real hang.
const WAKE_BOUND: Duration = Duration::from_secs(4);
/// Idle time that spans several probe intervals.
const PROBE_SPAN: Duration = Duration::from_millis(1500);

static SERIAL: Mutex<()> = Mutex::new(());
static NEXT_PAIR: AtomicU32 = AtomicU32::new(0);

fn serial() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

fn destroy(name: &str) {
    let _ = Command::new("ifconfig").args([name, "destroy"]).status();
}

/// A uniquely named feth pair that is destroyed on every exit path. It also
/// serializes the tests so CPU and timing measurements are not disturbed by
/// neighbors.
struct Pair {
    idx: u32,
    dev: String,
    peer: String,
    _serial: MutexGuard<'static, ()>,
}

impl Pair {
    fn new() -> Self {
        let guard = serial();
        let idx = NEXT_PAIR.fetch_add(1, Ordering::SeqCst);
        let base = 60 + 2 * idx;
        let pair = Pair {
            idx,
            dev: format!("feth{base}"),
            peer: format!("feth{}", base + 1),
            _serial: guard,
        };
        pair.destroy();
        pair
    }

    fn destroy(&self) {
        destroy(&self.dev);
        destroy(&self.peer);
    }

    /// Address of the TAP-side interface, unique per pair.
    fn addr(&self, host: u8) -> String {
        format!("10.90.{}.{host}", self.idx)
    }

    fn builder(&self) -> DeviceBuilder {
        DeviceBuilder::new()
            .name(self.dev.clone())
            .layer(Layer::L2)
            .peer_feth(self.peer.clone())
    }

    fn build(&self) -> Arc<AsyncDevice> {
        Arc::new(
            self.builder()
                .build_async()
                .expect("create async TAP (needs root)"),
        )
    }

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
    fn drop(&mut self) {
        self.destroy();
    }
}

fn is_fd_error(e: &io::Error) -> bool {
    e.raw_os_error().is_none() && e.to_string() == "fd error"
}

fn assert_enxio(e: &io::Error, ctx: &str) {
    assert_eq!(e.raw_os_error(), Some(libc::ENXIO), "{ctx}: {e:?}");
}

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

fn spawn_recv(dev: &Arc<AsyncDevice>) -> Receiver<io::Error> {
    let (tx, rx) = mpsc::channel();
    let dev = dev.clone();
    thread::spawn(move || {
        let _ = tx.send(recv_until_err(&dev));
    });
    rx
}

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
        Err(_) => panic!("{ctx}: did not finish within {HANG_GUARD:?}"),
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

#[ignore = "requires root and creates feth interfaces"]
#[test]
fn pending_recv_errors_after_feth_pair_destroyed() {
    let pair = Pair::new();
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
}

#[ignore = "requires root and creates feth interfaces"]
#[test]
fn recv_after_destroy_before_first_recv_errors_immediately() {
    let pair = Pair::new();
    let dev = pair.build();
    pair.destroy();
    let start = Instant::now();
    let rx = spawn_recv(&dev);
    let err = expect_err(&rx, "destroy before first recv");
    assert!(start.elapsed() < WAKE_BOUND, "took {:?}", start.elapsed());
    assert_enxio(&err, "destroy before first recv");
}

#[ignore = "requires root and creates feth interfaces"]
#[test]
fn many_parked_recvs_all_wake() {
    let pair = Pair::new();
    let dev = pair.build();
    let readers: Vec<_> = (0..16).map(|_| spawn_recv(&dev)).collect();
    thread::sleep(Duration::from_millis(500));
    pair.destroy();
    for (i, rx) in readers.iter().enumerate() {
        let err = expect_err(rx, &format!("parked reader {i}"));
        assert_enxio(&err, &format!("parked reader {i}"));
    }
}

#[ignore = "requires root and creates feth interfaces"]
#[test]
fn readable_then_try_recv_surfaces_the_error() {
    let pair = Pair::new();
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
}

#[ignore = "requires root and creates feth interfaces"]
#[test]
fn poll_recv_wakes_after_destroy() {
    let pair = Pair::new();
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
}

#[ignore = "requires root and creates feth interfaces"]
#[test]
fn recv_keeps_erroring_after_error_without_spinning() {
    let pair = Pair::new();
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
}

#[ignore = "requires root and creates feth interfaces"]
#[test]
fn dropped_parked_recv_does_not_break_the_next_wake() {
    let pair = Pair::new();
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
}

#[ignore = "requires root and creates feth interfaces"]
#[test]
fn queued_frames_then_destroy_end_in_error() {
    let pair = Pair::new();
    let dev = pair.build_with_ip();
    for _ in 0..5 {
        poke_neighbor(&pair);
        thread::sleep(Duration::from_millis(100));
    }
    pair.destroy();
    let rx = spawn_recv(&dev);
    let err = expect_err(&rx, "queued frames then destroy");
    assert_enxio(&err, "queued frames then destroy");
}

#[ignore = "requires root and creates feth interfaces"]
#[test]
fn send_after_destroy_reports_an_os_error() {
    let pair = Pair::new();
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
}

// ---------------------------------------------------------------------------
// Healthy device
// ---------------------------------------------------------------------------

#[ignore = "requires root and creates feth interfaces"]
#[test]
fn healthy_tap_keeps_receiving_across_the_probe_interval() {
    let pair = Pair::new();
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
                    if frame.len() >= 42 && frame[12..14] == [0x08, 0x06] && frame[38..42] == target
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
}

#[ignore = "requires root and creates feth interfaces"]
#[test]
fn writable_resolves_on_a_live_tap() {
    let pair = Pair::new();
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
}

#[ignore = "requires root and creates feth interfaces"]
#[test]
fn idle_recv_on_a_live_tap_does_not_spin() {
    let pair = Pair::new();
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
}

#[ignore = "requires root and creates utun interfaces"]
#[test]
fn utun_idle_recv_is_not_woken_by_the_probe() {
    let _serial = serial();
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
}
