package com.zdjizhi.etl.rule;

import com.zdjizhi.base.common.CommonInternalConfig;
import com.zdjizhi.base.rule.cache.CacheStore;
import com.zdjizhi.base.rule.cache.RuleCache;
import com.zdjizhi.base.rule.info.RuleInfo;
import com.zdjizhi.etl.utils.IocDarkWebUtils;
import com.zdjizhi.etl.utils.IocMalwareUtils;

import java.util.Optional;

public class RuleUpdateListener implements RuleCache.UpdateListener<Long, RuleInfo> {

    @Override
    public void onGet(Long key, RuleInfo value) {
    }

    @Override
    public void onPut(Long key, RuleInfo value) {
        if (value.getRuleType() == RuleInfo.RuleType.INDICATOR_MATCH) {
            RuleInfo.IndicatorMatchConfig ruleConfig = (RuleInfo.IndicatorMatchConfig) value.getRuleConfig();
            if (CommonInternalConfig.IOC_DARKWEB_TYPE.equals(ruleConfig.getLibrarySource())) {
                IocDarkWebUtils.updateInternalRuleId(key, value);
            }
            if (CommonInternalConfig.IOC_MALWARE_TYPE.equals(ruleConfig.getLibrarySource())) {
                IocMalwareUtils.updateInternalRuleId(key, value);
            }
        }
    }

    @Override
    public void onRemove(Long key) {
        Optional<RuleInfo> ruleInfo = RuleCache.getInstance().get(key);
        if (ruleInfo.isPresent()) {
            RuleInfo value = ruleInfo.get();
            if (value.getRuleType() == RuleInfo.RuleType.INDICATOR_MATCH) {
                RuleInfo.IndicatorMatchConfig ruleConfig = (RuleInfo.IndicatorMatchConfig) value.getRuleConfig();
                if (CommonInternalConfig.IOC_DARKWEB_TYPE.equals(ruleConfig.getLibrarySource())) {
                    IocDarkWebUtils.removeInternalRuleIds(key);
                }
                if (CommonInternalConfig.IOC_MALWARE_TYPE.equals(ruleConfig.getLibrarySource())) {
                    IocMalwareUtils.removeInternalRuleIds(key);
                }
            }
        }
    }

    @Override
    public void onClear() {
    }
}
