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

    private final Map<Long, Map<String, Object>> metadataMap = new HashMap<>();

    public void addMetadata(Map<String, Object> metadata) {
        Long id = Long.parseLong((String) metadata.get("kb_id"));
        metadataMap.put(id, metadata);
    }

    public boolean compare(CustomKnowledge customKnowledge) {
        Map<Long, Map<String, Object>> metadataMap = customKnowledge.metadataMap;
        if (this.metadataMap.size() != metadataMap.size()) {
            return false;
        }
        for (Long id : this.metadataMap.keySet()) {
            if (metadataMap.containsKey(id) && this.metadataMap.get(id).get("sha256").equals(metadataMap.get(id).get("sha256"))) {
                continue;
            }
            return false;

        }
        return true;
    }

    public Map<Long, byte[]> downloadKnowledge(int maxRetry) {
        Map<Long, byte[]> contents = new HashMap<>(this.metadataMap.size());
        this.metadataMap.values().forEach(metadata -> contents.put(
                Long.parseLong((String) metadata.get("kb_id")),
                downloadFile((String) metadata.get("path"), (String) metadata.get("sha256"), (Integer) metadata.get("is_valid"), maxRetry)));
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
