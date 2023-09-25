package com.zdjizhi.base.rule;

import org.joor.Reflect;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SimpleRuleManagerTest {

    private static final String ASYNC_FLUSH_RUNNING_FIELD_NAME = "ASYNC_FLUSH_RUNNING";

    private static final long INTERVAL_MS = 1000L;

    private SimpleRuleManager<String, Integer> simpleRuleManager;

    @Mock
    private RuleClient<String, Integer> mockClient;

    @Mock
    private AtomicBoolean mockAsyncFlushRunning;

    @Mock
    private Consumer<RuleClient.Updatable<String, Integer>> mockUpdate;

    @BeforeEach
    void setUp() {
        SimpleRuleManager<String, Integer> manager = new SimpleRuleManager<>(mockClient);
        Reflect.on(manager).set(ASYNC_FLUSH_RUNNING_FIELD_NAME, mockAsyncFlushRunning);
        simpleRuleManager = spy(manager);
    }

    @Test
    void testFlush() {
        simpleRuleManager.flush();

        verify(mockClient, times(1)).get();
        verify(simpleRuleManager, times((1))).update(any());
    }

    @Test
    void testFirstStartAsyncFlush() throws InterruptedException {

        Random random = new Random();
        int times = random.nextInt(20) + 10;

        doReturn(true).when(mockAsyncFlushRunning)
                .compareAndSet(false, true);

        simpleRuleManager.startAsyncFlush(INTERVAL_MS, mockUpdate);

        TimeUnit.MILLISECONDS.sleep(INTERVAL_MS * times);

        verify(mockClient, times(times + 1)).get();
        verify(simpleRuleManager, times(times + 1)).update(any());
    }

    @Test
    void testSecondStartAsyncFlush() {

        doReturn(false).when(mockAsyncFlushRunning)
                .compareAndSet(false, true);

        simpleRuleManager.startAsyncFlush(INTERVAL_MS, mockUpdate);

        verify(mockClient, never()).get();
        verify(simpleRuleManager, never()).update(any());
    }

    @Test
    void testAwaitAsyncFlushFinish() {
        doReturn(true).when(mockAsyncFlushRunning)
                .compareAndSet(false, true);

        Thread thread = new Thread(() -> {
            simpleRuleManager.startAsyncFlush(INTERVAL_MS, mockUpdate);
        });

        thread.start();

        assertTimeout(Duration.ofSeconds(10),
                () -> simpleRuleManager.awaitAsyncFlushFinish());

    }

    @Test
    void testTimeoutAwaitAsyncFlushFinish() {

        assertTimeout(Duration.ofSeconds(32),
                () -> simpleRuleManager.awaitAsyncFlushFinish());

    }

}