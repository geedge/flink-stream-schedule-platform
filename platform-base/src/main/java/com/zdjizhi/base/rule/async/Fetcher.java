package com.zdjizhi.base.rule.async;

import javax.annotation.Nullable;

/**
 * The Fetcher interface defines the contract for fetching data.
 *
 * @author chaoc
 * @since 23.10
 */
public interface Fetcher {

    /**
     * Opens the fetcher for fetching data.
     * Impl should perform any necessary initialization here.
     */
    void open();

    /**
     * A callback method that can be used to handle exceptions during data fetching.
     *
     * @param e The exception that occurred during data fetching, or null if no exception occurred.
     */
    default void callback(@Nullable RuntimeException e) {
        // Default impl does nothing. Imp can override this method as needed.
    }

    /**
     * Closes the fetcher and releases any allocated resources.
     * Impl should perform cleanup operations here.
     */
    void close();
}
