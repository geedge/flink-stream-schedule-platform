package com.zdjizhi.base.utils;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.zdjizhi.base.common.CommonConfig;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsUtils implements DFSUtils {
    private static final Log logger = LogFactory.get();
    private static FileSystem fileSystem;

    private static final String hdfsRootPath = CommonConfig.HDFS_PATH;

    public HdfsUtils() {
        Configuration configuration = new Configuration();
        try {
            //指定用户
            //配置hdfs相关信息
            configuration.set("fs.defaultFS","hdfs://ns1");
            configuration.set("hadoop.proxyuser.root.hosts","*");
            configuration.set("hadoop.proxyuser.root.groups","*");
            configuration.set("ha.zookeeper.quorum",CommonConfig.ZOOKEEPER_SERVERS);
            configuration.set("dfs.nameservices","ns1");
            configuration.set("dfs.ha.namenodes.ns1","nn1,nn2");
            configuration.set("dfs.namenode.rpc-address.ns1.nn1",CommonConfig.HDFS_URI_NS1);
            configuration.set("dfs.namenode.rpc-address.ns1.nn2",CommonConfig.HDFS_URI_NS2);
            configuration.set("dfs.client.failover.proxy.provider.ns1","org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
            System.setProperty("HADOOP_USER_NAME", CommonConfig.HDFS_USER);
            //创建fileSystem,用于连接hdfs
            fileSystem = FileSystem.get(configuration);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean isExists(String filePath) throws IOException {
        filePath = hdfsRootPath + filePath;
        return fileSystem.exists(new Path(filePath));
    }

    public byte[] getFileBytes(String filePath) {
        filePath = hdfsRootPath + filePath;
        byte[] bytes = new byte[0];
        try {
            FSDataInputStream open = fileSystem.open(new Path(filePath));
            bytes = IOUtils.toByteArray(open);
        } catch (IOException e) {
            logger.error("An I/O exception when files are download from HDFS. Message is :" + e.getMessage());
        }
        return bytes;
    }

    public void uploadFileByBytes(String filePath,byte[] bytes) throws IOException {
        filePath = hdfsRootPath + filePath;
        try (FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(filePath), true)) {
            fsDataOutputStream.write(bytes);
            fsDataOutputStream.flush();
        } catch (RuntimeException e) {
            logger.error("Uploading files to the HDFS is abnormal. Message is :" + e.getMessage());
        } catch (IOException e) {
            logger.error("An I/O exception when files are uploaded to HDFS. Message is :" + e.getMessage());
        }
    }

    public void rename(String src, String dst) throws IOException {
        src = hdfsRootPath + src;
        dst = hdfsRootPath + dst;
        fileSystem.rename(new Path(src),new Path(dst));
    }
}
