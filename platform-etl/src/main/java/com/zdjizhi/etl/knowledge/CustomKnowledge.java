package com.zdjizhi.etl.knowledge;

import cn.hutool.crypto.digest.DigestUtil;
import com.zdjizhi.base.utils.HttpClientUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class CustomKnowledge {

    private static final Logger logger = LoggerFactory.getLogger(CustomKnowledge.class);

    private final Map<String, KnowledgeManager.Knowledge> metadataMap = new HashMap<>();

    public void addMetadata(KnowledgeManager.Knowledge knowledge) {
        metadataMap.put(knowledge.getId(), knowledge);
    }

    public boolean compare(CustomKnowledge customKnowledge) {
        Map<String, KnowledgeManager.Knowledge> metadataMap = customKnowledge.metadataMap;
        if (this.metadataMap.size() != metadataMap.size()) {
            return false;
        }
        for (String id : this.metadataMap.keySet()) {
            if (metadataMap.containsKey(id) && this.metadataMap.get(id).getSha256().equals(metadataMap.get(id).getSha256())) {
                continue;
            }
            return false;
        }
        return true;
    }

    public Map<String, byte[]> downloadKnowledge(int maxRetry) {
        Map<String, byte[]> contents = new HashMap<>(this.metadataMap.size());
        this.metadataMap.values().forEach(knowledge -> contents.put(
                knowledge.getId(),
                downloadFile(knowledge.getPath(), knowledge.getSha256(), knowledge.getIsValid(), maxRetry)));
        return contents;
    }

    public static byte[] downloadFile(String fileUrl, String sha256, int isValid, int maxRetry) {
        byte[] content = new byte[0];
        if (isValid != 1) {
            return content;
        }
        String downloadFileSha256;
        int downloadCount = 0;
        do {
            content = downloadFile(fileUrl);
            downloadFileSha256 = DigestUtil.sha256Hex(content);
            downloadCount++;
            if (!Objects.equals(downloadFileSha256, sha256)) {
                logger.warn("warning: file url: " + fileUrl + " download failed, download size : " + content.length + ", download sha256:" + downloadFileSha256 + ", hope sha256 is " + sha256);
            }
        } while (!downloadFileSha256.equals(sha256) && downloadCount < maxRetry);
        if (downloadCount >= maxRetry) {
            int length = Math.min(1000, content.length);
            logger.warn("warning: file url: " + fileUrl + " download more than specified number of times, the part of content is :" + new String(content, 0, length, StandardCharsets.UTF_8));
        }
        return content;
    }

    private static byte[] downloadFile(String fileUrl) {
        byte[] content = new byte[0];
        try {
            long start = System.currentTimeMillis();
            //Header header = new BasicHeader("token", CommonConfig.HOS_TOKEN);
            HttpClientUtils httpClientUtils = new HttpClientUtils();
            //content = httpClientUtils.httpGetInputStream(fileUrl, header);
            content = httpClientUtils.httpGetInputStream(fileUrl);
            long end = System.currentTimeMillis();
            logger.info("download file " + fileUrl + " finished, speed " + (end - start) + " ms");
        } catch (Exception e) {
            logger.error("download file " + fileUrl + " error: " + e.getMessage(), e);
        }
        return content;
    }
}
