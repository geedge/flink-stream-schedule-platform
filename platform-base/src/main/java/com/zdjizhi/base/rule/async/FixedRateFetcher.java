package com.zdjizhi.base.rule.async;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Impl of a fixed-rate Fetcher.
 * Responsible for executing specified business code at fixed time intervals.
 *
 * @author chaoc
 * @since 1.0
 */
public class FixedRateFetcher implements Fetcher, ScheduledSupport {

    // The runnable code to be executed at fixed intervals.
    private final Runnable runnable;

    //  The time interval, in milliseconds, at which the code should be executed.
    private final long rateIntervalMs;

    // The scheduled future for managing execution.
    private ScheduledFuture<?> scheduledFuture;

    /**
     * Constructs a FixedRateFetcher with the provided runnable code and rate interval.
     *
     * @param runnable       The code to be executed.
     * @param rateIntervalMs The time interval, in milliseconds, at which the code should be executed.
     */
    public FixedRateFetcher(final Runnable runnable, final long rateIntervalMs) {
        this.runnable = runnable;
        this.rateIntervalMs = rateIntervalMs;
    }

    /**
     * Opens the FixedRateFetcher, scheduling the execution of the runnable code at fixed intervals.
     */
    @Override
    public void open() {
        scheduledFuture = scheduledExecutorService()
                .scheduleAtFixedRate(() -> {
                    try {
                        runnable.run();
                    } catch (RuntimeException e) {
                        callback(e);
                    }
                    // invoke callback
                    callback(null);
                }, 0, rateIntervalMs, TimeUnit.MILLISECONDS);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                close();
            } catch (Exception e) {
                throw new RuntimeException("Fetcher close error.", e);
            }
        }));
    }

    /**
     * Closes the FixedRateFetcher, canceling the scheduled future.
     *
     * @throws Exception if an error occurs while closing.
     */
    @Override
    public void close() throws Exception {
        scheduledFuture.cancel(true);
    }
}
