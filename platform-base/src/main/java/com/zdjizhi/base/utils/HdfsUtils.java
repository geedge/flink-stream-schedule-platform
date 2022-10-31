package com.zdjizhi.base.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HdfsUtils {

    private static FileSystem fileSystem;

    static {
        Configuration configuration = new Configuration();
        try {
            fileSystem = FileSystem.get(configuration);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean isExists(String filePath) throws IOException {
        return fileSystem.exists(new Path(filePath));
    }

    public static byte[] getFileBytes(String filePath) throws IOException {
        FSDataInputStream open = fileSystem.open(new Path(filePath));
        byte[] bytes = new byte[open.available()];
        open.read(0,bytes,0, open.available());
        return bytes;
    }

    public static void uploadFileByBytes(String filePath,byte[] bytes) throws IOException {
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(filePath), false);
        fsDataOutputStream.write(bytes);
    }

    public static void rename(String src, String dst) throws IOException {
        fileSystem.rename(new Path(src),new Path(dst));
    }

}
