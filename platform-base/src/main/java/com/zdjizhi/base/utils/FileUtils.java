package com.zdjizhi.base.utils;

import cn.hutool.crypto.digest.DigestUtil;
import com.zdjizhi.base.common.CommonConfig;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class FileUtils {

    private static DFSUtils dFSUtils;

    private static final String CLUSTER = "CLUSTER";
    private static final String SINGLE = "SINGLE";

    static {
        if (CLUSTER.equals(CommonConfig.CLUSTER_OR_SINGLE)){
            dFSUtils = new HdfsUtils();
        }
        if (SINGLE.equals(CommonConfig.CLUSTER_OR_SINGLE)){
            dFSUtils = new FileByteUtils();
        }
    }

    public static byte[] getFileBytes(String fileRelativePath) {
        long start = System.currentTimeMillis();
        byte[] fileBytes = dFSUtils.getFileBytes(fileRelativePath);
        long end = System.currentTimeMillis();
        System.out.println("file " + fileRelativePath + " download finished, speed " + (end - start) + " ms");
        return fileBytes;
    }

    public static void uploadFileByBytes(String fileRelativePath, byte[] bytes) throws IOException {
        long start = System.currentTimeMillis();
        dFSUtils.uploadFileByBytes(fileRelativePath, bytes);
        long end = System.currentTimeMillis();
        System.out.println("file " + fileRelativePath + " upload finished, speed " + (end - start) + " ms");
    }

    public static byte[] downloadFile(String fileName, String sha256) {
        byte[] content;
        String downloadFileSha256;
        int downloadCount = 0;
        do {
            content = getFileBytes(fileName);
            downloadFileSha256 = DigestUtil.sha256Hex(content);
            downloadCount++;
        } while (!downloadFileSha256.equals(sha256) && downloadCount < CommonConfig.KNOWLEDGE_FILE_CHECK_NUMBER);
        if (!downloadFileSha256.equals(sha256)) {
            content = new byte[0];
            System.out.println("broadcast stream download file " + fileName + " error, attempted more than " + CommonConfig.KNOWLEDGE_FILE_CHECK_NUMBER + " times.");
        }
        return content;
    }
}
