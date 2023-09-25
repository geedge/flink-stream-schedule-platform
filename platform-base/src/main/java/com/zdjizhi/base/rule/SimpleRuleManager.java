package com.zdjizhi.base.rule;

import com.zdjizhi.base.rule.async.AsyncFlusher;
import com.zdjizhi.base.rule.async.Fetcher;
import com.zdjizhi.base.rule.async.FixedRateFetcher;

import javax.annotation.Nullable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * SimpleRuleManager is an impl of the RuleManager interface for managing rules of type K, V.
 *
 * @param <K> The type of the key.
 * @param <V> The type of the value.
 * @author chaoc
 * @since 1.0
 */
public class SimpleRuleManager<K, V> implements RuleManager<K, V> {

    // Atomic boolean to track if asynchronous flush is running.
    private final static AtomicBoolean ASYNC_FLUSH_RUNNING = new AtomicBoolean(false);

    // CountDownLatch for asynchronous flush synchronization.
    private final CountDownLatch ASYNC_FLUSH_LATCH = new CountDownLatch(1);

    private final RuleClient<K, V> client;

    private Fetcher fetcher;

    // The consumer for handling updates to the rule.
    private Consumer<RuleClient.Updatable<K, V>> update;

    /**
     * Creates a new SimpleRuleManager with the specified RuleClient.
     *
     * @param client The RuleClient to use for rule management.
     */
    public SimpleRuleManager(final RuleClient<K, V> client) {
        this.client = client;
    }

    @Override
    public void flush() {
        final AsyncFlusher.DefaultAsyncFlusher<K, V> flusher =
                new AsyncFlusher.DefaultAsyncFlusher<>(client, this);
        flusher.run();
    }

    @Override
    public void startAsyncFlush(final long intervalMs, final Consumer<RuleClient.Updatable<K, V>> update) {
        if (ASYNC_FLUSH_RUNNING.compareAndSet(false, true)) {
            this.update = update;
            this.fetcher = new FixedRateFetcher(
                    new AsyncFlusher.DefaultAsyncFlusher<>(client, this), intervalMs) {
                @Override
                public void callback(@Nullable RuntimeException e) {
                    ASYNC_FLUSH_LATCH.countDown();
                }
            };
            fetcher.open();
        }
    }

    @Override
    public void awaitAsyncFlushFinish() throws InterruptedException {
        this.ASYNC_FLUSH_LATCH.await();
    }

    public void stopAsyncFlush() throws Exception {
        if (this.fetcher != null) {
            this.fetcher.close();
        }
    }

    @Override
    public void update(RuleClient.Updatable<K, V> updateable) {
        if (update != null) {
            update.accept(updateable);
        }
    }
}
