package com.zdjizhi.etl.utils.tag;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.rule.info.RuleInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BaseTagUtils {

    private static final Logger logger = LoggerFactory.getLogger(BaseTagUtils.class);
    private static final Map<Long, List<Long>> libraryIdRuleIdMap = new HashMap<>();

    private static final Map<Long, LibraryType> libraryTypeMap = new HashMap<>();

    public static void addLibraryType(Long libraryId, LibraryType libraryType) {
        libraryTypeMap.put(libraryId, libraryType);
    }

    public static void updateInternalRuleId(Long key, RuleInfo ruleInfo) {
        logger.info("tagUtils update rule: " + ruleInfo.getName() + ":" + ruleInfo.getRuleId());
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
        logger.info("tagUtils remove rule : " + ruleInfo.getName() + ":" + ruleInfo.getRuleId());
        RuleInfo.RuleConfig ruleConfig = ruleInfo.getRuleConfig();
        if (ruleConfig instanceof RuleInfo.IndicatorMatchConfig) {
            Long libraryId = ((RuleInfo.IndicatorMatchConfig) ruleConfig).getLibraryId();
            if (libraryIdRuleIdMap.containsKey(libraryId)) {
                libraryIdRuleIdMap.get(libraryId).remove(key);
            }
        }
    }

    protected static void setRuleIdAndIocType(CnRecordLog entity, Node node) {
        node.knowledgeIds.forEach(libraryId -> {
            String libraryType = libraryTypeMap.get(libraryId).name;
            if (libraryIdRuleIdMap.containsKey(libraryId)) {
                for (Long ruleId : libraryIdRuleIdMap.get(libraryId)) {
                    entity.getRule_id_list().add(ruleId);
                    entity.getIoc_type_list().add(libraryType);
                }
            }
        });
    }


    protected enum LibraryType {
        APP("app"),
        DOMAIN("domain"),
        IP("ip");

        private final String name;

        LibraryType(String name) {
            this.name = name;
        }

        public String getName() {
            return this.name;
        }
    }
}
