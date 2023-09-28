package com.zdjizhi.base.rule.cache;

import com.zdjizhi.base.rule.info.RuleInfo;

/**
 * Caching rules using a rule-id and RuleInfo values.
 *
 * @author chaoc
 * @since 1.0
 */
public final class RuleCache extends AbstractCacheStore<Long, RuleInfo> {

    /**
     * Get the singleton instance of RuleCache.
     *
     * @return The singleton instance of RuleCache.
     */
    public static RuleCache getInstance() {
        return SingletonHolder.INSTANCE;
    }

    // ======================================================================================
    // ----------------------------------- private helper -----------------------------------

    // Inner class to hold the singleton instance of RuleCache.
    private static final class SingletonHolder {
        public final static RuleCache INSTANCE = new RuleCache();
    }
}
