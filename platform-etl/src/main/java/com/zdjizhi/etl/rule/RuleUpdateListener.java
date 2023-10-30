package com.zdjizhi.etl.rule;

import com.zdjizhi.base.common.CommonInternalConfig;
import com.zdjizhi.base.rule.cache.CacheStore;
import com.zdjizhi.base.rule.cache.RuleCache;
import com.zdjizhi.base.rule.info.RuleInfo;
import com.zdjizhi.etl.utils.IocDarkWebUtils;
import com.zdjizhi.etl.utils.IocMalwareUtils;
import com.zdjizhi.etl.utils.TagUtils;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RuleUpdateListener implements RuleCache.UpdateListener<Long, RuleInfo> {

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();



    @Override
    public void onGet(Long key, RuleInfo value) {
    }

    public void processCurrentRuleInfos() {
        try {
            lock.writeLock().lock();
            RuleCache instance = RuleCache.getInstance();
            Collection<RuleInfo> ruleInfos = instance.getAll();
            ruleInfos.forEach(ruleInfo -> doPut(ruleInfo.getRuleId(), ruleInfo));
            System.out.println("rule listener init process " + ruleInfos.size() + " rules");
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void onPut(Long key, RuleInfo value) {
        try {
            lock.writeLock().lock();
            doPut(key, value);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void doPut(Long key, RuleInfo value) {
        if (value.getRuleType() == RuleInfo.RuleType.INDICATOR_MATCH) {
            RuleInfo.IndicatorMatchConfig ruleConfig = (RuleInfo.IndicatorMatchConfig) value.getRuleConfig();
            if (CommonInternalConfig.IOC_DARKWEB_TYPE.equals(ruleConfig.getLibrarySource())) {
                IocDarkWebUtils.updateInternalRuleId(key, value);
            } else if (CommonInternalConfig.IOC_MALWARE_TYPE.equals(ruleConfig.getLibrarySource())) {
                IocMalwareUtils.updateInternalRuleId(key, value);
            } else {
                TagUtils.updateInternalRuleId(key, value);
            }

        }
    }

    @Override
    public void onRemove(Long key, RuleInfo ruleInfo) {
        try {
            lock.writeLock().lock();
            doRemove(key, ruleInfo);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void doRemove(Long key, RuleInfo value) {
        if (value != null) {
            if (value.getRuleType() == RuleInfo.RuleType.INDICATOR_MATCH) {
                RuleInfo.IndicatorMatchConfig ruleConfig = (RuleInfo.IndicatorMatchConfig) value.getRuleConfig();
                if (CommonInternalConfig.IOC_DARKWEB_TYPE.equals(ruleConfig.getLibrarySource())) {
                    IocDarkWebUtils.removeInternalRuleId(key, value);
                } else if (CommonInternalConfig.IOC_MALWARE_TYPE.equals(ruleConfig.getLibrarySource())) {
                    IocMalwareUtils.removeInternalRuleId(key, value);
                } else {
                    TagUtils.removeInternalRuleId(key, value);
                }
            }
        }
    }

    @Override
    public void onClear() {
    }
}
