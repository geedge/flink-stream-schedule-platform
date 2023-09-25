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
}
