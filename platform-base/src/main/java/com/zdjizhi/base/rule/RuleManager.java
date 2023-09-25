package com.zdjizhi.base.rule;

import java.util.function.Consumer;

/**
 * The RuleManager interface defines methods for managing rules associated with keys and values.
 *
 * @param <K> The type of keys.
 * @param <V> The type of values.
 * @author chaoc
 * @since 1.0
 */
public interface RuleManager<K, V> {

    /**
     * Updates the rule with the provided updateable object.
     *
     * @param updateable The updateable object containing the rule update.
     */
    void update(final RuleClient.Updateable<K, V> updateable);

    /**
     * Starts asynchronous rule flush with the specified interval and update consumer.
     *
     * @param intervalMs The interval in milliseconds for rule flush.
     * @param update     The consumer for handling updates to the rule.
     * @throws Exception Thrown if an error occurs during the start of asynchronous flushing.
     */
    void startAsyncFlush(long intervalMs, Consumer<RuleClient.Updateable<K, V>> update) throws Exception;

    /**
     * Waits for the asynchronous rule flush to finish.
     *
     * @throws InterruptedException if interrupted while waiting.
     */
    void awaitAsyncFlushFinish() throws InterruptedException;

    /**
     * Stops the asynchronous rule flush.
     *
     * @throws Exception if an error occurs while stopping.
     */
    void stopAsyncFlush() throws Exception;

    /**
     * Flushes the rules synchronously.
     */
    void flush();
}

