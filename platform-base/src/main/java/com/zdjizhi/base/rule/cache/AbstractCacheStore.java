package com.zdjizhi.base.rule.cache;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * An impl of the CacheStore interface providing basic functionality for caching.
 *
 * @param <K> The type of keys in the cache.
 * @param <V> The type of values in the cache.
 * @author chaoc
 * @since 23.10
 */
public abstract class AbstractCacheStore<K, V> implements CacheStore<K, V> {

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    // A list of listeners to be notified when cache updates occur.
    private final List<UpdateListener<K, V>> listeners = new ArrayList<>();

    // The map that stores key-value pairs in the cache.
    private final Map<K, V> valueMap = new HashMap<>();

    @Override
    public Optional<V> get(K key) {
        try {
            lock.readLock().lock();
            V v = valueMap.get(key);
            listeners.forEach(listener ->
                    listener.onGet(key, v));
            return Optional.ofNullable(v);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void put(K key, V value) {
        try {
            lock.writeLock().lock();
            valueMap.put(key, value);
            listeners.forEach(listener ->
                    listener.onPut(key, value));
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        try {
            lock.writeLock().lock();
            map.forEach((key, value) -> {
                valueMap.put(key, value);
                listeners.forEach(listener ->
                        listener.onPut(key, value));
            });
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void remove(K key) {
        try {
            lock.writeLock().lock();
            V v = valueMap.remove(key);
            listeners.forEach(listener ->
                    listener.onRemove(key));
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public long size() {
        try {
            lock.readLock().lock();
            return valueMap.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void clear() {
        try {
            lock.writeLock().lock();
            valueMap.clear();
            listeners.forEach(UpdateListener::onClear);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Adds a listener to be notified of cache updates.
     *
     * @param listener The listener to add.
     */
    public void addListener(final UpdateListener<K, V> listener) {
        this.listeners.add(listener);
    }
}
