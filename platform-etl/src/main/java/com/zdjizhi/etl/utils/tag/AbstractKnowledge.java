package com.zdjizhi.etl.utils.tag;

import com.zdjizhi.base.common.CnRecordLog;

import java.util.Map;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2023/12/29 9:44
 */
public abstract class AbstractKnowledge {

    public abstract CustomKnowledgeUtils.LibraryType getType();

    public abstract void parseContents(Map<Long, byte[]> contents);

    public abstract void setTag(CnRecordLog entity);

    public void updateKnowledge(Map<Long, byte[]> contents) {
        contents.keySet().forEach(libraryId -> CustomKnowledgeUtils.addLibraryType(libraryId, getType()));
        parseContents(contents);
    }
}
