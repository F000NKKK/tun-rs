//! Privileged macOS regression test for the async TAP detach wake-up.
//!
//! When the feth pair backing a TAP device is destroyed, XNU raises no poll
//! event on the BPF descriptor. A pending `recv` must still return an error
//! (ENXIO) in bounded time instead of hanging forever.
//!
//! Run with root on macOS:
//!   sudo -E cargo test --test macos_tap_detach --features async_tokio -- --ignored
#![cfg(all(target_os = "macos", feature = "async_tokio"))]

use std::process::Command;
use std::sync::Arc;
use std::time::Duration;
use tun_rs::DeviceBuilder;

fn destroy(name: &str) {
    let _ = Command::new("ifconfig").args([name, "destroy"]).status();
}

#[ignore = "requires root and creates feth interfaces"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pending_recv_errors_after_feth_pair_destroyed() {
    let (dev, peer) = ("feth41", "feth42");
    destroy(dev);
    destroy(peer);
    let device = Arc::new(
        DeviceBuilder::new()
            .name(dev)
            .layer(tun_rs::Layer::L2)
            .peer_feth(peer)
            .build_async()
            .expect("create async TAP (needs root)"),
    );

    let reader = {
        let device = device.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 2048];
            device.recv(&mut buf).await
        })
    };

    // Let the reader park in the readiness wait, then destroy the pair.
    tokio::time::sleep(Duration::from_millis(500)).await;
    destroy(dev);
    destroy(peer);

    let result = tokio::time::timeout(Duration::from_secs(5), reader)
        .await
        .expect("recv hung after the feth pair was destroyed (upstream #179)")
        .expect("reader task panicked");
    let err = result.expect_err("recv on a destroyed feth must fail");
    assert_eq!(
        err.raw_os_error(),
        Some(libc::ENXIO),
        "unexpected error: {err:?}"
    );
}
