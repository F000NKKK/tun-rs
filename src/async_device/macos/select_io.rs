use crate::DeviceImpl;
use bytes::buf::UninitSlice;
use std::future::Future;
use std::io;
use std::io::{IoSlice, IoSliceMut};
use std::ops::Deref;
use std::os::fd::AsRawFd;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

pub(crate) struct NonBlockingDevice {
    device: DeviceImpl,
    shutdown_event: EventFd,
}
impl NonBlockingDevice {
    pub(crate) fn new_dev(device: DeviceImpl) -> io::Result<Self> {
        device.set_nonblocking(true)?;
        Ok(Self {
            device,
            shutdown_event: EventFd::new()?,
        })
    }

    pub fn try_recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        self.device.recv(buf)
    }

    pub fn try_recv_uninit(&self, buf: &mut UninitSlice) -> io::Result<usize> {
        self.device.recv_uninit(buf)
    }

    pub fn try_send(&self, buf: &[u8]) -> io::Result<usize> {
        self.device.send(buf)
    }
    pub fn try_recv_vectored(&self, bufs: &mut [IoSliceMut<'_>]) -> io::Result<usize> {
        self.device.recv_vectored(bufs)
    }
    pub fn try_send_vectored(&self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
        self.device.send_vectored(bufs)
    }

    pub(crate) fn shutdown(&self) -> io::Result<()> {
        self.shutdown_event.wake()
    }
}

impl NonBlockingDevice {
    pub fn wait_writable(&self, cancel_event: Option<libc::c_int>) -> io::Result<()> {
        self.wait(libc::POLLOUT, cancel_event)
    }
    pub fn wait_readable(&self, cancel_event: Option<libc::c_int>) -> io::Result<()> {
        self.wait(libc::POLLIN, cancel_event)
    }
    fn wait(
        &self,
        device_events: libc::c_short,
        cancel_event: Option<libc::c_int>,
    ) -> io::Result<()> {
        let fd = if device_events & libc::POLLOUT != 0 {
            self.device.writable_fd()
        } else {
            self.device.as_raw_fd()
        };
        let event_fd = self.shutdown_event.as_event_fd();
        let mut fds = Vec::with_capacity(if cancel_event.is_some() { 3 } else { 2 });
        fds.push(libc::pollfd {
            fd,
            events: device_events,
            revents: 0,
        });
        fds.push(libc::pollfd {
            fd: event_fd,
            events: libc::POLLIN,
            revents: 0,
        });
        if let Some(cancel_event) = cancel_event {
            fds.push(libc::pollfd {
                fd: cancel_event,
                events: libc::POLLIN,
                revents: 0,
            });
        }
        // A destroyed feth pair raises no poll event on XNU, so TAP waits use
        // a finite timeout and probe whether the BPF fd is still attached.
        let result = if self.device.needs_detach_probe() {
            poll_with_probe(&mut fds, DETACH_PROBE_INTERVAL, || {
                self.device.is_detached()
            })?
        } else {
            let result = unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as libc::nfds_t, -1) };
            if result < 0 {
                return Err(io::Error::last_os_error());
            }
            result
        };
        if result == 0 {
            // Detached: report readiness so the next read/write surfaces the
            // kernel's ENXIO instead of hanging.
            return Ok(());
        }
        if fds[1].revents & libc::POLLIN != 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "close"));
        }
        if cancel_event.is_some() && fds[2].revents & libc::POLLIN != 0 {
            return Err(io::Error::new(io::ErrorKind::Interrupted, "cancel"));
        }
        if fds[0].revents & device_events != 0 {
            return Ok(());
        }
        Err(io::Error::other("fd error"))
    }
}

/// Interval between detach probes while a TAP wait is idle (about 4 wakeups
/// per second, so idle waits never turn into a busy loop).
const DETACH_PROBE_INTERVAL: Duration = Duration::from_millis(250);

/// Polls `fds` with a finite timeout, calling `is_detached` after every idle
/// timeout. Returns the number of ready descriptors, or `0` when
/// `is_detached` reported `true`. `EINTR` is retried.
fn poll_with_probe(
    fds: &mut [libc::pollfd],
    interval: Duration,
    mut is_detached: impl FnMut() -> bool,
) -> io::Result<libc::c_int> {
    let timeout_ms = interval.as_millis().clamp(1, i32::MAX as u128) as libc::c_int;
    loop {
        let result = unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as libc::nfds_t, timeout_ms) };
        if result < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(err);
        }
        if result > 0 {
            return Ok(result);
        }
        if is_detached() {
            return Ok(0);
        }
    }
}

struct EventFd(libc::c_int, libc::c_int);
impl EventFd {
    fn new() -> io::Result<Self> {
        let mut fds = [0 as libc::c_int; 2];
        if unsafe { libc::pipe(fds.as_mut_ptr()) } == -1 {
            return Err(io::Error::last_os_error());
        }
        let read_fd = fds[0];
        let write_fd = fds[1];
        if let Err(e) = set_pipe_fd_flags(read_fd).and_then(|_| set_pipe_fd_flags(write_fd)) {
            unsafe {
                let _ = libc::close(read_fd);
                let _ = libc::close(write_fd);
            }
            return Err(e);
        }
        Ok(Self(read_fd, write_fd))
    }
    fn wake(&self) -> io::Result<()> {
        let buf: [u8; 8] = 2u64.to_ne_bytes();
        let res = unsafe { libc::write(self.1, buf.as_ptr() as *const libc::c_void, buf.len()) };
        if res == -1 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::WouldBlock {
                Ok(())
            } else {
                Err(err)
            }
        } else {
            Ok(())
        }
    }
    fn wait_timeout(&self, timeout: Duration) -> io::Result<()> {
        let mut fds = [libc::pollfd {
            fd: self.0,
            events: libc::POLLIN,
            revents: 0,
        }];
        let timeout_ms = timeout.as_millis().min(i32::MAX as u128) as libc::c_int;
        let res = unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as libc::nfds_t, timeout_ms) };
        if res < 0 {
            Err(io::Error::last_os_error())
        } else if res == 0 {
            Err(io::Error::from(io::ErrorKind::TimedOut))
        } else {
            Ok(())
        }
    }
    fn as_event_fd(&self) -> libc::c_int {
        self.0
    }
}

fn set_pipe_fd_flags(fd: libc::c_int) -> io::Result<()> {
    unsafe {
        let flags = libc::fcntl(fd, libc::F_GETFL);
        if flags < 0 {
            return Err(io::Error::last_os_error());
        }
        if libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) < 0 {
            return Err(io::Error::last_os_error());
        }
        let fd_flags = libc::fcntl(fd, libc::F_GETFD);
        if fd_flags < 0 {
            return Err(io::Error::last_os_error());
        }
        if libc::fcntl(fd, libc::F_SETFD, fd_flags | libc::FD_CLOEXEC) < 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}
impl Drop for EventFd {
    fn drop(&mut self) {
        unsafe {
            let _ = libc::close(self.0);
            let _ = libc::close(self.1);
        }
    }
}

pub struct AsyncDevice {
    inner: Arc<NonBlockingDevice>,
    recv_task_lock: Arc<Mutex<Option<blocking::Task<io::Result<()>>>>>,
    send_task_lock: Arc<Mutex<Option<blocking::Task<io::Result<()>>>>>,
}

impl Deref for AsyncDevice {
    type Target = DeviceImpl;
    fn deref(&self) -> &Self::Target {
        &self.inner.device
    }
}
impl Drop for AsyncDevice {
    fn drop(&mut self) {
        _ = self.inner.shutdown();
    }
}
impl AsyncDevice {
    pub(crate) fn new_dev(device: DeviceImpl) -> io::Result<AsyncDevice> {
        let inner = Arc::new(NonBlockingDevice::new_dev(device)?);
        Ok(AsyncDevice {
            inner,
            recv_task_lock: Arc::new(Mutex::new(None)),
            send_task_lock: Arc::new(Mutex::new(None)),
        })
    }
}
impl AsyncDevice {
    pub async fn readable(&self) -> io::Result<()> {
        let device = self.inner.clone();
        let mut canceller = Canceller::new_cancelable()?;
        let (cancel_guard, exit_guard) = canceller.guard(device);
        blocking::unblock(move || {
            exit_guard
                .call(|device, cancel_event| device.wait_readable(Some(cancel_event.as_event_fd())))
        })
        .await?;
        std::mem::forget(cancel_guard);
        Ok(())
    }
    pub async fn writable(&self) -> io::Result<()> {
        let device = self.inner.clone();
        let mut canceller = Canceller::new_cancelable()?;
        let (cancel_guard, exit_guard) = canceller.guard(device);
        blocking::unblock(move || {
            exit_guard
                .call(|device, cancel_event| device.wait_writable(Some(cancel_event.as_event_fd())))
        })
        .await?;
        std::mem::forget(cancel_guard);
        Ok(())
    }
    pub fn poll_readable(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut guard = self.recv_task_lock.lock().unwrap();
        let mut task = if let Some(task) = guard.take() {
            task
        } else {
            let device = self.inner.clone();
            blocking::unblock(move || device.wait_readable(None))
        };
        match Pin::new(&mut task).poll(cx) {
            Poll::Ready(rs) => {
                drop(guard);
                match rs {
                    Ok(_) => Poll::Ready(Ok(())),
                    Err(e) => Poll::Ready(Err(e)),
                }
            }
            Poll::Pending => {
                guard.replace(task);
                Poll::Pending
            }
        }
    }
    pub fn poll_recv(&self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
        loop {
            match self.try_recv(buf) {
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {}
                rs => return Poll::Ready(rs),
            }
            match self.poll_readable(cx)? {
                Poll::Ready(_) => {}
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }
    }
    #[allow(dead_code)]
    pub(crate) fn poll_recv_uninit(
        &self,
        cx: &mut Context<'_>,
        buf: &mut UninitSlice,
    ) -> Poll<io::Result<usize>> {
        loop {
            match self.inner.try_recv_uninit(buf) {
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {}
                rs => return Poll::Ready(rs),
            }
            match self.poll_readable(cx)? {
                Poll::Ready(_) => {}
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }
    }
    pub fn poll_writable(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut guard = self.send_task_lock.lock().unwrap();
        let mut task = if let Some(task) = guard.take() {
            task
        } else {
            let device = self.inner.clone();
            blocking::unblock(move || device.wait_writable(None))
        };
        match Pin::new(&mut task).poll(cx) {
            Poll::Ready(rs) => match rs {
                Ok(_) => Poll::Ready(Ok(())),
                Err(e) => Poll::Ready(Err(e)),
            },
            Poll::Pending => {
                guard.replace(task);
                Poll::Pending
            }
        }
    }
    pub fn poll_send(&self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        loop {
            match self.try_send(buf) {
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {}
                rs => return Poll::Ready(rs),
            }
            match self.poll_writable(cx)? {
                Poll::Ready(_) => {}
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }
    }
    pub async fn recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            match self.try_recv(buf) {
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {}
                rs => return rs,
            }
            self.readable().await?;
        }
    }
    pub fn try_recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.try_recv(buf)
    }
    pub async fn send(&self, buf: &[u8]) -> io::Result<usize> {
        loop {
            match self.try_send(buf) {
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {}
                rs => return rs,
            }
            self.writable().await?;
        }
    }
    pub fn try_send(&self, buf: &[u8]) -> io::Result<usize> {
        self.inner.try_send(buf)
    }
    pub async fn recv_vectored(&self, bufs: &mut [IoSliceMut<'_>]) -> io::Result<usize> {
        loop {
            match self.try_recv_vectored(bufs) {
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {}
                rs => return rs,
            }
            self.readable().await?;
        }
    }
    pub fn try_recv_vectored(&self, bufs: &mut [IoSliceMut<'_>]) -> io::Result<usize> {
        self.inner.try_recv_vectored(bufs)
    }
    pub async fn send_vectored(&self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
        loop {
            match self.try_send_vectored(bufs) {
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {}
                rs => return rs,
            }
            self.writable().await?;
        }
    }
    pub fn try_send_vectored(&self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
        self.inner.try_send_vectored(bufs)
    }
}

struct ExitSignalGuard {
    device: Option<Arc<NonBlockingDevice>>,
    cancel_event_handle: Arc<EventFd>,
    exit_event: Arc<EventFd>,
}
impl Drop for ExitSignalGuard {
    fn drop(&mut self) {
        drop(self.device.take());
        _ = self.exit_event.wake();
    }
}
impl ExitSignalGuard {
    pub fn call<R>(
        &self,
        mut op: impl FnMut(&NonBlockingDevice, &EventFd) -> io::Result<R>,
    ) -> io::Result<R> {
        if let Some(device) = &self.device {
            op(device, &self.cancel_event_handle)
        } else {
            unreachable!()
        }
    }
}

struct Canceller {
    exit_event_handle: Arc<EventFd>,
    cancel_event_handle: Arc<EventFd>,
}

impl Canceller {
    fn new_cancelable() -> io::Result<Self> {
        Ok(Self {
            exit_event_handle: Arc::new(EventFd::new()?),
            cancel_event_handle: Arc::new(EventFd::new()?),
        })
    }

    fn guard(
        &mut self,
        device_impl: Arc<NonBlockingDevice>,
    ) -> (CancelWaitGuard<'_>, ExitSignalGuard) {
        (
            CancelWaitGuard {
                exit_event_handle: &self.exit_event_handle,
                cancel_event_handle: &self.cancel_event_handle,
            },
            ExitSignalGuard {
                device: Some(device_impl),
                exit_event: self.exit_event_handle.clone(),
                cancel_event_handle: self.cancel_event_handle.clone(),
            },
        )
    }
}

struct CancelWaitGuard<'a> {
    exit_event_handle: &'a Arc<EventFd>,
    cancel_event_handle: &'a Arc<EventFd>,
}

impl Drop for CancelWaitGuard<'_> {
    fn drop(&mut self) {
        if self.cancel_event_handle.wake().is_ok() {
            _ = self
                .exit_event_handle
                .wait_timeout(Duration::from_millis(1))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    fn idle_pipe() -> EventFd {
        EventFd::new().unwrap()
    }

    /// An idle wait must probe at the finite interval, never spin. A loaded
    /// runner can only make the gaps longer, so the check is a lower bound on
    /// each gap with no upper bound on wall time.
    #[test]
    fn idle_wait_probes_at_bounded_rate() {
        const PROBES: u32 = 5;
        let pipe = idle_pipe();
        let mut fds = [libc::pollfd {
            fd: pipe.as_event_fd(),
            events: libc::POLLIN,
            revents: 0,
        }];
        let mut probes = 0u32;
        let mut gaps = Vec::new();
        let mut last = Instant::now();
        let result = poll_with_probe(&mut fds, DETACH_PROBE_INTERVAL, || {
            probes += 1;
            let now = Instant::now();
            gaps.push(now.duration_since(last));
            last = now;
            probes >= PROBES
        })
        .unwrap();
        assert_eq!(result, 0);
        assert_eq!(gaps.len(), PROBES as usize);
        // A spinning loop probes back to back; a real wait takes about the
        // interval, so half of it separates the two with wide margin.
        for (i, gap) in gaps.iter().enumerate() {
            assert!(
                *gap >= DETACH_PROBE_INTERVAL / 2,
                "probe {i} came after only {gap:?}"
            );
        }
    }

    /// Without a detach the probe keeps running: the loop only ends on a
    /// detach or readiness, so it cannot return early on its own.
    #[test]
    fn probe_runs_until_detached() {
        let pipe = idle_pipe();
        let mut fds = [libc::pollfd {
            fd: pipe.as_event_fd(),
            events: libc::POLLIN,
            revents: 0,
        }];
        let mut probes = 0u32;
        let result = poll_with_probe(&mut fds, Duration::from_millis(20), || {
            probes += 1;
            probes == 3
        })
        .unwrap();
        assert_eq!(result, 0);
        assert_eq!(probes, 3);
    }

    #[test]
    fn readiness_wins_over_probe() {
        let pipe = idle_pipe();
        pipe.wake().unwrap();
        let mut fds = [libc::pollfd {
            fd: pipe.as_event_fd(),
            events: libc::POLLIN,
            revents: 0,
        }];
        let result = poll_with_probe(&mut fds, DETACH_PROBE_INTERVAL, || {
            panic!("probe must not run when an fd is ready")
        })
        .unwrap();
        assert_eq!(result, 1);
    }
}
