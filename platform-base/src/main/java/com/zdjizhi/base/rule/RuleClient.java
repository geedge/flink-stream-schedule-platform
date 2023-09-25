package com.zdjizhi.base.rule;

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
    final class SimpleUpdatable<K, V> implements Updatable<K, V> {

        private final List<V> addList;
        private final List<V> updateList;
        private final List<K> deleteIds;

        public SimpleUpdatable(
                final List<V> addList,
                final List<V> updateList,
                final List<K> deleteIds) {
            this.addList = addList;
            this.updateList = updateList;
            this.deleteIds = deleteIds;
        }

        @Override
        public List<V> getUpdateList() {
            return addList;
        }

        @Override
        public List<V> getAddList() {
            return updateList;
        }

        @Override
        public List<K> getDeleteKeys() {
            return deleteIds;
        }
    }
}
