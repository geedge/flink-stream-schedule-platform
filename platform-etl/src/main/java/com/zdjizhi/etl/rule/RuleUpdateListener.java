package com.zdjizhi.etl.rule;

import com.zdjizhi.base.common.CommonInternalConfig;
import com.zdjizhi.base.rule.cache.RuleCache;
import com.zdjizhi.base.rule.info.RuleInfo;
import com.zdjizhi.etl.utils.IocDarkWebUtils;
import com.zdjizhi.etl.utils.IocMalwareUtils;
import com.zdjizhi.etl.utils.tag.BaseTagUtils;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RuleUpdateListener implements RuleCache.UpdateListener<Long, RuleInfo> {

    private final Logger logger = LoggerFactory.getLogger(RuleUpdateListener.class);

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private final Configuration configuration;

    private final static AtomicBoolean IS_INIT = new AtomicBoolean(false);
    private final static CountDownLatch ASYNC_WAIT = new CountDownLatch(1);

    public RuleUpdateListener(Configuration configuration) {
        this.configuration = configuration;
    }

    public void registerListener() throws InterruptedException {
        if (IS_INIT.compareAndSet(false, true)) {
            try {
                RuleCache.getInstance().addListener(this);
                this.processCurrentRuleInfos();
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                ASYNC_WAIT.countDown();
            }
        }
        ASYNC_WAIT.await();
    }



    @Override
    public void onGet(Long key, RuleInfo value) {
    }

    public void processCurrentRuleInfos() {
        try {
            lock.writeLock().lock();
            RuleCache instance = RuleCache.getInstance();
            Collection<RuleInfo> ruleInfos = instance.getAll();
            ruleInfos.forEach(ruleInfo -> doPut(ruleInfo.getRuleId(), ruleInfo));
            logger.info("rule listener init process " + ruleInfos.size() + " rules");
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
            if (configuration.get(CommonInternalConfig.IOC_DARKWEB_TYPE).equals(ruleConfig.getLibrarySource())) {
                IocDarkWebUtils.updateInternalRuleId(key, value);
            } else if (configuration.get(CommonInternalConfig.IOC_MALWARE_TYPE).equals(ruleConfig.getLibrarySource())) {
                IocMalwareUtils.updateInternalRuleId(key, value);
            } else {
                BaseTagUtils.updateInternalRuleId(key, value);
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
                if (configuration.get(CommonInternalConfig.IOC_DARKWEB_TYPE).equals(ruleConfig.getLibrarySource())) {
                    IocDarkWebUtils.removeInternalRuleId(key, value);
                } else if (configuration.get(CommonInternalConfig.IOC_MALWARE_TYPE).equals(ruleConfig.getLibrarySource())) {
                    IocMalwareUtils.removeInternalRuleId(key, value);
                } else {
                    BaseTagUtils.removeInternalRuleId(key, value);
                }
            }
        }
    }

    @Override
    public void onClear() {
    }
}
