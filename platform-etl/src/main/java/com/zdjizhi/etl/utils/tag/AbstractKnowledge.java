package com.zdjizhi.etl.utils.tag;

import com.zdjizhi.base.common.CnRecordLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2023/12/29 9:44
 */
public abstract class AbstractKnowledge {

    private static final Logger logger = LoggerFactory.getLogger(AbstractKnowledge.class);

    public abstract CustomKnowledgeUtils.LibraryType getType();

    public abstract void parseContents(Map<String, byte[]> contents);

    public abstract void setTag(CnRecordLog entity);

    public void updateKnowledge(Map<String, byte[]> contents) {
        contents = contents.entrySet()
                .stream()
                .filter(entry -> checkId(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (existing, replacement) -> existing, HashMap::new));
        contents.keySet().forEach(libraryId -> CustomKnowledgeUtils.addLibraryType(convertId(libraryId), getType()));
        parseContents(contents);
    }

    private boolean checkId(String id) {
        try {
            convertId(id);
        } catch (Exception e) {
            logger.warn("转换知识库文件id到知识库id异常,无效的知识库文件id:" + id);
            return false;
        }
        return true;
    }

    /**
     * 和知识库同步服务的转换规则保持一致
     *
     * @param id
     * @return
     */
    protected Long convertId(String id) {
        return Long.parseLong(id);
    }
}
