use async_speed_limit::Limiter;
use async_speed_limit::clock::{Clock, StandardClock};
use async_speed_limit::limiter::Consume;
use futures::prelude::*;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite};

/// Apply the given bps limit on writes to this stream.
pub(crate) struct RateLimitedStream<S, C = StandardClock>
where
    C: Clock,
{
    inner: S,
    limiter: Limiter<C>,
    waiter: Option<Consume<C, ()>>,
}

impl<S, C> RateLimitedStream<S, C>
where
    C: Clock,
{
    pub(crate) fn new(inner: S, limiter: Limiter<C>) -> Self {
        Self {
            inner,
            limiter,
            waiter: None,
        }
    }
}

impl<S, C> AsyncRead for RateLimitedStream<S, C>
where
    S: AsyncRead + Unpin,
    C: Clock,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<S, C> AsyncWrite for RateLimitedStream<S, C>
where
    S: AsyncWrite + Unpin,
    C: Clock,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if let Some(waiter) = &mut self.waiter {
            let res = Pin::new(waiter).poll(cx);
            if res.is_pending() {
                return Poll::Pending;
            }
            self.waiter = None;
        }

        let res = Pin::new(&mut self.inner).poll_write(cx, buf);
        if let Poll::Ready(Ok(len)) = &res {
            if *len > 0 {
                self.waiter = Some(self.limiter.consume(*len));
            }
        }

        res
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }

    // Note: poll_write_vectored disabled even if the underlying
    // stream supports it; writes go through poll_write
}

impl<S: Unpin, C: Clock> Unpin for RateLimitedStream<S, C> {}

#[cfg(test)]
mod tests {
    use super::*;
    use async_speed_limit::clock::{ManualClock, Nanoseconds};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    #[tokio::test]
    async fn rate_limited_stream() -> anyhow::Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let mut a = TcpStream::connect(listener.local_addr()?).await?;
        let (b, _) = listener.accept().await?;
        let limiter = Limiter::<ManualClock>::new(3.0); // 3 bps

        let limiter_for_reader = limiter.clone();
        let reader = tokio::spawn(async move {
            let mut buf = vec![0; 6];

            let c = a.read(&mut buf).await?;
            assert_eq!(c, 3);
            assert_eq!(buf, b"123\0\0\0");

            // Advance time to let through more bytes.
            tokio::task::yield_now().await;
            limiter_for_reader
                .clock()
                .set_time(Nanoseconds(1_000_000_000));
            tokio::task::yield_now().await;

            let c = a.read(&mut buf).await?;
            assert_eq!(c, 3);
            assert_eq!(buf, b"456\0\0\0");

            // Advance time to let through more bytes.
            tokio::task::yield_now().await;
            limiter_for_reader
                .clock()
                .set_time(Nanoseconds(2_000_000_000));
            tokio::task::yield_now().await;

            let c = a.read(&mut buf).await?;
            assert_eq!(c, 3);
            assert_eq!(buf, b"789\0\0\0");

            Ok::<(), anyhow::Error>(())
        });

        let mut limited_b = RateLimitedStream::new(b, limiter.clone());
        let writer = tokio::spawn(async move {
            limited_b.write_all(b"123").await?;
            // First write goes through immediately.

            limited_b.write_all(b"456").await?;
            // Second write had to wait for the clock to advance.
            assert_eq!(limited_b.limiter.clock().now(), Nanoseconds(1_000_000_000));

            limited_b.write_all(b"789").await?;
            // Third write had to wait for the clock to advance a 2nd time.
            assert_eq!(limited_b.limiter.clock().now(), Nanoseconds(2_000_000_000));

            limited_b.shutdown().await?;

            Ok::<(), anyhow::Error>(())
        });

        reader.await??;
        writer.await??;

        assert_eq!(limiter.total_bytes_consumed(), 9);
        assert_eq!(limiter.clock().now(), Nanoseconds(2_000_000_000));

        Ok(())
    }
}
