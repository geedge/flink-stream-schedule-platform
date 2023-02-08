package com.zdjizhi.etl.reader;

import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.utils.FileByteUtils;
import com.zdjizhi.base.utils.HdfsUtils;
import com.zdjizhi.etl.utils.*;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

public class JsonReader {
    private java.io.Reader appsktReader = null;
    private java.io.Reader whoisReader = null;
    private BufferedReader appsktBufferedReader = null;
    private BufferedReader whoisBufferedReader = null;

    public void loadJson(){

        try {
            if ("CLUSTER".equals(CommonConfig.CLUSTER_OR_SINGLE)) {
                //todo app
                byte[] appsktBytes = HdfsUtils.getFileBytes(CommonConfig.HDFS_PATH + "app_sketch_labeled.json");
                if (appsktBytes != null) {
                    ByteArrayInputStream appsktInputStream = new ByteArrayInputStream(appsktBytes);
                    this.appsktReader = new InputStreamReader(appsktInputStream);
                    this.appsktBufferedReader = new BufferedReader(appsktReader);
                }
                //todo whois
                byte[] whoisBytes = HdfsUtils.getFileBytes(CommonConfig.HDFS_PATH + "XJ_top.json");
                if (whoisBytes != null) {
                    ByteArrayInputStream whoisInputStream = new ByteArrayInputStream(whoisBytes);
                    this.whoisReader = new InputStreamReader(whoisInputStream);
                    this.whoisBufferedReader = new BufferedReader(whoisReader);
                }
            }else if ("SINGLE".equals(CommonConfig.CLUSTER_OR_SINGLE)){
                byte[] appsktBytes = FileByteUtils.getFileBytes(CommonConfig.DOWNLOAD_PATH + "app_sketch_labeled.json");
                if (appsktBytes != null) {
                    ByteArrayInputStream appsktInputStream = new ByteArrayInputStream(appsktBytes);
                    this.appsktReader = new InputStreamReader(appsktInputStream);
                    this.appsktBufferedReader = new BufferedReader(appsktReader);
                }
                byte[] whoisBytes = FileByteUtils.getFileBytes(CommonConfig.DOWNLOAD_PATH + "XJ_top.json");
                if (whoisBytes != null) {
                    ByteArrayInputStream whoisInputStream = new ByteArrayInputStream(whoisBytes);
                    this.whoisReader = new InputStreamReader(whoisInputStream);
                    this.whoisBufferedReader = new BufferedReader(whoisReader);
                }
            }


            AppUtils.readJson(appsktBufferedReader);
            WhoisUtils.readJson(whoisBufferedReader);


        } catch (IOException e) {
            e.printStackTrace();
        }

    }



    public void updateJson(Map<String, byte[]> knowledgeFileCache) {
        try {
            ByteArrayInputStream appsktInputStream;
            ByteArrayInputStream whoisInputStream;
            for (Map.Entry<String, byte[]> entry : knowledgeFileCache.entrySet()) {
                if ("app_sketch_labeled.json".equals(entry.getKey() )&& entry.getValue() !=null) {
                    appsktInputStream = new ByteArrayInputStream(entry.getValue());
                    this.appsktReader = new InputStreamReader(appsktInputStream);
                    this.appsktBufferedReader = new BufferedReader(appsktReader);
                    AppUtils.readJson(appsktBufferedReader);
                }
                if ("XJ_top.json".equals(entry.getKey() )&& entry.getValue() !=null) {
                    whoisInputStream = new ByteArrayInputStream(entry.getValue());
                    this.whoisReader = new InputStreamReader(whoisInputStream);
                    this.whoisBufferedReader = new BufferedReader(whoisReader);
                    WhoisUtils.readJson(whoisBufferedReader);
                }
            }
        } catch (Exception e) {
            System.out.println(e.toString());
        }

    }
}
