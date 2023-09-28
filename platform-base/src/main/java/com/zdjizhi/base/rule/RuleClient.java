package com.zdjizhi.base.rule;

import lombok.ToString;

import java.util.List;

/**
 * The RuleClient interface defines the contract for a client that interacts with rules.
 *
 * @param <ID> The type of rule ID.
 * @param <T>  The type of rule.
 * @author chaoc
 * @since 1.0
 */
public interface RuleClient<ID, T> {

    /**
     * Retrieves an updatable instance of rules.
     *
     * @return An Updatable instance for managing rules.
     */
    Updatable<ID, T> get();

    /**
     * The Updatable interface represents a set of rules that can be updated.
     *
     * @param <ID> The type of rule ID.
     * @param <T>  The type of rule.
     */
    interface Updatable<ID, T> {

        /**
         * Gets the list of rules to be updated.
         *
         * @return A list of rules to be updated.
         */
        List<T> getUpdateList();

        /**
         * Gets the list of rules to be added.
         *
         * @return A list of rules to be added.
         */
        List<T> getAddList();

        /**
         * Gets the list of keys of rules to be deleted.
         *
         * @return A list of keys of rules to be deleted.
         */
        List<ID> getDeleteKeys();
    }

    /**
     * A default impl of The Updatable interface.
     */
    @ToString
    final class SimpleUpdatable<K, V> implements Updatable<K, V> {

        private final List<? extends V> addList;
        private final List<? extends V> updateList;
        private final List<? extends K> deleteIds;

        public SimpleUpdatable(
                final List<? extends V> addList,
                final List<? extends V> updateList,
                final List<? extends K> deleteIds) {
            this.addList = addList;
            this.updateList = updateList;
            this.deleteIds = deleteIds;
        }

        @SuppressWarnings("unchecked")
        @Override
        public List<V> getUpdateList() {
            return (List<V>) updateList;
        }

        @SuppressWarnings("unchecked")
        @Override
        public List<V> getAddList() {
            return (List<V>) addList;
        }

        @SuppressWarnings("unchecked")
        @Override
        public List<K> getDeleteKeys() {
            return (List<K>) deleteIds;
        }
    }
}
