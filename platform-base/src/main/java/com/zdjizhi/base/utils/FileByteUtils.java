package com.zdjizhi.base.utils;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IoUtil;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.config.Configs;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class FileByteUtils implements DFSUtils {

    private final Logger logger = LoggerFactory.getLogger(FileByteUtils.class);

    private static final String localRootPath = Configs.get(CommonConfig.DOWNLOAD_PATH);

    public FileByteUtils() {
        if (!FileUtil.exist(localRootPath)) {
            FileUtil.mkdir(localRootPath);
        }
    }

    public byte[] getFileBytes(String filePath) {
        filePath = localRootPath + filePath;
        FileInputStream fis = null;
        byte[] data = new byte[0];
        try {
            File file = new File(filePath);
            fis = new FileInputStream(file);
            data = IOUtils.toByteArray(fis);
        } catch (IOException e) {
            logger.error("download file " + filePath + " error, error message: " + e.getMessage(), e);
        } finally {
            IOUtils.closeQuietly(fis);
        }
        return data;
    }

    @Override
    public void uploadFileByBytes(String filePath, byte[] bytes) throws IOException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        uploadFile(filePath, byteArrayInputStream);
    }

    public void uploadFile(String filePath, InputStream inputStream) throws IOException {
        filePath = localRootPath + filePath;
        FileOutputStream fos = new FileOutputStream(filePath);
        IoUtil.copy(inputStream, fos);
    }
}
