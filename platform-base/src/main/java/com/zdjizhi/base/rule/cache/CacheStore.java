package com.zdjizhi.base.rule.cache;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

/**
 * Defines methods for caching key-value pairs and provides an optional update listener.
 *
 * @param <K> The type of keys.
 * @param <V> The type of values.
 * @author chaoc
 * @since 23.10
 */
public interface CacheStore<K, V> {

    /**
     * Get all data.
     *
     * @return All data.
     */
    Collection<V> getAll();

    /**
     * Retrieves the value associated with the specified key.
     *
     * @param key The key to retrieve the value for.
     * @return An Optional containing the value if found, otherwise empty.
     */
    Optional<V> get(final K key);

    /**
     * Associates the specified value with the specified key in the cache.
     *
     * @param key   The key to associate the value with.
     * @param value The value to be stored.
     */
    void put(final K key, V value);

    /**
     * Associates all the key-value pairs from the given map in the cache.
     *
     * @param map The map containing key-value pairs to be stored in the cache.
     */
    void putAll(final Map<? extends K, ? extends V> map);

    /**
     * Removes the key-value pair associated with the specified key from the cache.
     *
     * @param key The key whose associated value is to be removed.
     */
    void remove(final K key);

    /**
     * Returns the number of key-value pairs in the cache.
     *
     * @return The number of key-value pairs in the cache.
     */
    long size();

    /**
     * Clears all key-value pairs from the cache.
     */
    void clear();

    /**
     * The UpdateListener interface defines callback methods for cache updates.
     *
     * @param <K> The type of keys.
     * @param <V> The type of values.
     */
    interface UpdateListener<K, V> {

        /**
         * Called when a value is retrieved from the cache.
         *
         * @param key   The key for which the value is retrieved.
         * @param value The retrieved value.
         */
        default void onGet(final K key, final V value) {
        }

        /**
         * Called when a value is put into the cache.
         *
         * @param key   The key for which the value is put.
         * @param value The value that is put into the cache.
         */
        default void onPut(final K key, V value) {
        }

        /**
         * Called when a key-value pair is removed from the cache.
         *
         * @param key The key for which the value is removed.
         * @param v   The value is removed.
         */
        default void onRemove(final K key, final V v) {
        }

        /**
         * Called when the cache is cleared, removing all key-value pairs.
         */
        default void onClear() {
        }
    }
}

