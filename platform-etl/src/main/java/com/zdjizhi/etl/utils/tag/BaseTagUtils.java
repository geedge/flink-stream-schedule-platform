package com.zdjizhi.etl.utils.tag;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.rule.info.RuleInfo;
import com.zdjizhi.etl.utils.TimeUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BaseTagUtils {
    private static final Map<Long, List<Long>> libraryIdRuleIdMap = new HashMap<>();

    public static void updateInternalRuleId(Long key, RuleInfo ruleInfo) {
        System.out.println(TimeUtils.getCurrentTime() + ": tagUtils update rule: " + ruleInfo.getName() + ":" + ruleInfo.getRuleId());
        long ruleId = ruleInfo.getRuleId();
        RuleInfo.RuleConfig ruleConfig = ruleInfo.getRuleConfig();
        if (ruleConfig instanceof RuleInfo.IndicatorMatchConfig) {
            Long libraryId = ((RuleInfo.IndicatorMatchConfig) ruleConfig).getLibraryId();
            if (libraryIdRuleIdMap.containsKey(libraryId)) {
                libraryIdRuleIdMap.get(libraryId).add(ruleId);
            } else {
                List<Long> ruleIdList = new ArrayList<>();
                ruleIdList.add(ruleId);
                libraryIdRuleIdMap.put(libraryId, ruleIdList);
            }
        }
    }

    public static void removeInternalRuleId(Long key, RuleInfo ruleInfo) {
        System.out.println(TimeUtils.getCurrentTime() + ": tagUtils remove rule : " + ruleInfo.getName() + ":" + ruleInfo.getRuleId());
        RuleInfo.RuleConfig ruleConfig = ruleInfo.getRuleConfig();
        if (ruleConfig instanceof RuleInfo.IndicatorMatchConfig) {
            Long libraryId = ((RuleInfo.IndicatorMatchConfig) ruleConfig).getLibraryId();
            if (libraryIdRuleIdMap.containsKey(libraryId)) {
                libraryIdRuleIdMap.get(libraryId).remove(key);
            }
        }
    }

    protected static void setRuleIdAndIocType(CnRecordLog entity, Node node) {
        for (int i = 0; i < node.knowledgeIds.size(); i++) {
            Long libraryId = node.knowledgeIds.get(i);
            String libraryType = node.knowledgeTypes.get(i);
            if (libraryIdRuleIdMap.containsKey(libraryId)) {
                for (Long ruleId : libraryIdRuleIdMap.get(libraryId)) {
                    entity.getRule_id_list().add(ruleId);
                    entity.getIoc_type_list().add(libraryType);
                }
            }
        }

    }
}
