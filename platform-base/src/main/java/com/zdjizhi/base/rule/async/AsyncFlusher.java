package com.zdjizhi.base.rule.async;

import com.zdjizhi.base.rule.RuleClient;
import com.zdjizhi.base.rule.RuleManager;

/**
 * An asynchronous flusher interface.
 *
 * @author chaoc
 * @since 1.0
 */
public interface AsyncFlusher extends Runnable {

    /**
     * Callback method executed after each flush operation.
     */
    default void callback() {
    }

    @Override
    default void run() {
        execute();
        callback();
    }

    /**
     * Method to be implemented for scheduling execution.
     */
    void execute();

    /**
     * Default implementation of an asynchronous flusher.
     *
     * @param <K> The data ID type obtained by the client.
     * @param <V> The data type obtained by the client.
     */
    final class DefaultAsyncFlusher<K, V> implements AsyncFlusher {

        private final RuleClient<K, V> client;

        private final RuleManager<K, V> manager;

        /**
         * Constructs a DefaultAsyncFlusher with the provided RuleClient and RuleManager.
         *
         * @param client  The RuleClient used to retrieve data.
         * @param manager The RuleManager used to update data.
         */
        public DefaultAsyncFlusher(RuleClient<K, V> client, RuleManager<K, V> manager) {
            this.client = client;
            this.manager = manager;
        }

        @Override
        public void execute() {
            RuleClient.Updatable<K, V> kvUpdatable = client.get();
            manager.update(kvUpdatable);
        }
    }
}
