package com.zdjizhi.base.rule.async;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.annotation.Nullable;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class FixedRateFetcherTest {

    private final long rateIntervalMs = 1000L;

    private CountDownLatch latch;

    @Mock
    private Runnable mockRunnable;

    private FixedRateFetcher fetcher;

    @BeforeEach
    void setUp() {
        latch = new CountDownLatch(1);
        fetcher = spy(new FixedRateFetcher(mockRunnable, rateIntervalMs) {
            @Override
            public void callback(@Nullable RuntimeException e) {
                latch.countDown();
            }
        });

    }

    @Test
    void testOpen() throws InterruptedException {
        fetcher.open();
        latch.await();

        verify(fetcher, times(1)).scheduledExecutorService();
        verify(mockRunnable, times(1)).run();
    }

    @Test
    void testOpenWithException() throws InterruptedException {

        RuntimeException mockException = mock(RuntimeException.class);

        doThrow(mockException).when(mockRunnable).run();

        fetcher.open();
        latch.await();

        verify(fetcher, times(1)).scheduledExecutorService();
        verify(mockRunnable, times(1)).run();
        verify(fetcher, times(1)).callback(mockException);
    }

    @Test
    void testClose() throws Exception {

        fetcher.open();

        assertDoesNotThrow(() -> fetcher.close());
        verify(fetcher, times(1)).open();
        verify(fetcher, times(1)).scheduledExecutorService();
        verify(fetcher, times(1)).close();
    }

    @Test
    void testCloseWithException() throws Exception {

        assertThrows(IllegalStateException.class, () -> fetcher.close());
        verify(fetcher, times(1)).close();

    }

}