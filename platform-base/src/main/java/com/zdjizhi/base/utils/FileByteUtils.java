package com.zdjizhi.base.utils;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IoUtil;
import com.zdjizhi.base.common.CommonConfig;

import java.io.*;

public class FileByteUtils implements DFSUtils {

    private static final String localRootPath = CommonConfig.DOWNLOAD_PATH;

    public FileByteUtils() {
        if (!FileUtil.exist(localRootPath)) {
            FileUtil.mkdir(localRootPath);
        }
    }

    public byte[] getFileBytes(String filePath) throws IOException {
        filePath = localRootPath + filePath;
        File file = new File(filePath);
        FileInputStream fis = new FileInputStream(file);
        ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
        byte[] b = new byte[1024];
        int n;
        while ((n = fis.read(b)) != -1) {
            bos.write(b, 0, n);
        }
        fis.close();
        byte[] data = bos.toByteArray();
        bos.close();
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
