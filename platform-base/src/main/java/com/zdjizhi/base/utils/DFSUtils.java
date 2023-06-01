package com.zdjizhi.base.utils;

import java.io.IOException;

public interface DFSUtils {

    byte[] getFileBytes(String filePath);

    void uploadFileByBytes(String filePath, byte[] bytes) throws IOException;
}
