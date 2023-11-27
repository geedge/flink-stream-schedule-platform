package com.zdjizhi.base.utils;

import cn.hutool.crypto.digest.DigestUtil;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.config.Configs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class FileUtils {

    private final Logger logger = LoggerFactory.getLogger(FileUtils.class);

    private static DFSUtils dFSUtils;

    public FileUtils(String clusterOrSingle) {
        if ("CLUSTER".equals(clusterOrSingle)) {
            dFSUtils = new HdfsUtils();
        }
        if ("SINGLE".equals(clusterOrSingle)) {
            dFSUtils = new FileByteUtils();
        }
    }

    public byte[] getFileBytes(String fileRelativePath) {
        long start = System.currentTimeMillis();
        byte[] fileBytes = dFSUtils.getFileBytes(fileRelativePath);
        long end = System.currentTimeMillis();
        logger.info("file " + fileRelativePath + " download finished, speed " + (end - start) + " ms");
        return fileBytes;
    }

    public void uploadFileByBytes(String fileRelativePath, byte[] bytes) throws IOException {
        long start = System.currentTimeMillis();
        dFSUtils.uploadFileByBytes(fileRelativePath, bytes);
        long end = System.currentTimeMillis();
        logger.info("file " + fileRelativePath + " upload finished, speed " + (end - start) + " ms");
    }

    public byte[] downloadFile(String fileName, String sha256) {
        byte[] content;
        String downloadFileSha256;
        int downloadCount = 0;
        do {
            content = getFileBytes(fileName);
            downloadFileSha256 = DigestUtil.sha256Hex(content);
            downloadCount++;
        } while (!downloadFileSha256.equals(sha256) && downloadCount < Configs.get(CommonConfig.KNOWLEDGE_FILE_CHECK_NUMBER));
        if (!downloadFileSha256.equals(sha256)) {
            content = new byte[0];
            logger.info("broadcast stream download file " + fileName + " error, attempted more than " + Configs.get(CommonConfig.KNOWLEDGE_FILE_CHECK_NUMBER) + " times.");
        }
        return content;
    }
}
