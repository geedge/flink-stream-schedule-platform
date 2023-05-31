package com.zdjizhi.base.utils;

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

    public static byte[] getFileBytes(String fileRelativePath) throws IOException {
        return dFSUtils.getFileBytes(fileRelativePath);
    }

    public static void uploadFileByBytes(String fileRelativePath, byte[] bytes) throws IOException {
        dFSUtils.uploadFileByBytes(fileRelativePath, bytes);
    }
}
