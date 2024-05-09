package com.zdjizhi.base.rule.async;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Impl of a fixed-rate Fetcher.
 * Responsible for executing specified business code at fixed time intervals.
 *
 * @author chaoc
 * @since 23.10
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
        if (scheduledFuture != null) {
            return;
        }
        scheduledFuture = scheduledExecutorService()
                .scheduleWithFixedDelay(() -> {
                    try {
                        runnable.run();
                        // invoke callback
                        callback(null);
                    } catch (RuntimeException e) {
                        callback(e);
                    }
                }, 0, rateIntervalMs, TimeUnit.MILLISECONDS);
        Runtime.getRuntime().addShutdownHook(new Thread(this::close));
    }

    /**
     * Closes the FixedRateFetcher, canceling the scheduled future.
     *
     * @throws IllegalStateException if an error occurs while closing.
     */
    @Override
    public void close() {
        if (scheduledFuture == null) {
            throw new IllegalStateException("Schedule task be not started.");
        }
        scheduledFuture.cancel(true);
    }
}
