package com.zdjizhi.etl.utils.tag;

import com.zdjizhi.base.common.CnRecordLog;

import java.util.Map;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2023/12/29 9:56
 */
public class CustomKnowledgeManager {

    private static final AppKnowledge APP_KNOWLEDGE = new AppKnowledge();
    private static final DomainKnowledge DOMAIN_KNOWLEDGE = new DomainKnowledge();
    private static final IpKnowledge IP_KNOWLEDGE = new IpKnowledge();

    public static void updateAppKnowledge(Map<String, byte[]> contents) {
        APP_KNOWLEDGE.updateKnowledge(contents);
    }

    public static void updateDomainKnowledge(Map<String, byte[]> contents) {
        DOMAIN_KNOWLEDGE.updateKnowledge(contents);
    }

    public static void updateIpKnowledge(Map<String, byte[]> contents) {
        IP_KNOWLEDGE.updateKnowledge(contents);
    }

    public static void setTag(CnRecordLog entity) {
        APP_KNOWLEDGE.setTag(entity);
        DOMAIN_KNOWLEDGE.setTag(entity);
        IP_KNOWLEDGE.setTag(entity);
    }
}
