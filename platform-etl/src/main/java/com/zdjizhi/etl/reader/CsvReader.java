package com.zdjizhi.etl.reader;

import com.maxmind.db.CHMCache;
import com.maxmind.db.Reader;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.common.CustomFile;
import com.zdjizhi.base.utils.FileByteUtils;
import com.zdjizhi.base.utils.HdfsUtils;
import com.zdjizhi.etl.utils.*;

import java.io.*;
import java.util.List;

public class CsvReader {
    private java.io.Reader websktReader = null;
    private java.io.Reader dnsReader = null;
    private java.io.Reader appsktReader = null;
    private java.io.Reader whoisReader = null;
    private java.io.Reader icpReader = null;
    private java.io.Reader linkReader = null;
    private java.io.Reader ispReader = null;
    private java.io.Reader idcReader = null;
    private java.io.Reader internalReader = null;

    private String clusterOrSingle;

    public CsvReader(String clusterOrSingle) {
        this.clusterOrSingle = clusterOrSingle;
    }

    public void loadCsv() {

        try {
            if ("CLUSTER".equals(clusterOrSingle)){
                //todo webskt
                byte[] websktBytes = HdfsUtils.getFileBytes(CommonConfig.HDFS_PATH + "websktDB_full-20211123-101740.csv");
                if (websktBytes != null) {
                    ByteArrayInputStream websktInputStream = new ByteArrayInputStream(websktBytes);
                    this.websktReader = new InputStreamReader(websktInputStream);
                }
                //todo dnsskt
                byte[] dnsBytes = HdfsUtils.getFileBytes(CommonConfig.HDFS_PATH + "webskt_default_dns_server_info_full_20220427.csv");
                if (dnsBytes != null) {
                    ByteArrayInputStream dnsInputStream = new ByteArrayInputStream(dnsBytes);
                    this.dnsReader = new InputStreamReader(dnsInputStream);
                }
                //todo icp
                byte[] icpBytes = HdfsUtils.getFileBytes(CommonConfig.HDFS_PATH + "icp.csv");
                if (icpBytes != null) {
                    ByteArrayInputStream icpInputStream = new ByteArrayInputStream(icpBytes);
                    this.icpReader = new InputStreamReader(icpInputStream);
                }
                //todo link
                byte[] linkBytes = HdfsUtils.getFileBytes(CommonConfig.HDFS_PATH + "link_direction.csv");
                if (linkBytes != null) {
                    ByteArrayInputStream linkInputStream = new ByteArrayInputStream(linkBytes);
                    this.linkReader = new InputStreamReader(linkInputStream);
                }
                //todo isp
                byte[] ispBytes = HdfsUtils.getFileBytes(CommonConfig.HDFS_PATH + "xjlt_ip_location_20220422.csv");
                if (ispBytes != null) {
                    ByteArrayInputStream ispInputStream = new ByteArrayInputStream(ispBytes);
                    this.ispReader = new InputStreamReader(ispInputStream);
                }
                //todo idc
                byte[] idcBytes = HdfsUtils.getFileBytes(CommonConfig.HDFS_PATH + "xjlt_idc_renter_20220422.csv");
                if (idcBytes != null) {
                    ByteArrayInputStream idcInputStream = new ByteArrayInputStream(idcBytes);
                    this.idcReader = new InputStreamReader(idcInputStream);
                }
                //todo internal
                byte[] internalBytes = HdfsUtils.getFileBytes(CommonConfig.HDFS_PATH + "xjlt_idc_internal_ip_20220422.csv");
                if (internalBytes != null) {
                    ByteArrayInputStream internalInputStream = new ByteArrayInputStream(internalBytes);
                    this.internalReader = new InputStreamReader(internalInputStream);
                }
            }else if ("SINGLE".equals(clusterOrSingle)){
                byte[] websktBytes = FileByteUtils.getFileBytes(CommonConfig.DOWNLOAD_PATH + "websktDB_full-20211123-101740.csv");
                if (websktBytes != null) {
                    ByteArrayInputStream websktInputStream = new ByteArrayInputStream(websktBytes);
                    this.websktReader = new InputStreamReader(websktInputStream);
                }
                byte[] dnsBytes = FileByteUtils.getFileBytes(CommonConfig.DOWNLOAD_PATH + "webskt_default_dns_server_info_full_20220427.csv");
                if (dnsBytes != null) {
                    ByteArrayInputStream dnsInputStream = new ByteArrayInputStream(dnsBytes);
                    this.dnsReader = new InputStreamReader(dnsInputStream);
                }
                byte[] icpBytes = FileByteUtils.getFileBytes(CommonConfig.DOWNLOAD_PATH + "icp.csv");
                if (icpBytes != null) {
                    ByteArrayInputStream icpInputStream = new ByteArrayInputStream(icpBytes);
                    this.icpReader = new InputStreamReader(icpInputStream);
                }

                byte[] linkBytes = FileByteUtils.getFileBytes(CommonConfig.DOWNLOAD_PATH + "link_direction.csv");
                if (linkBytes != null) {
                    ByteArrayInputStream linkInputStream = new ByteArrayInputStream(linkBytes);
                    this.linkReader = new InputStreamReader(linkInputStream);
                }
                byte[] ispBytes = FileByteUtils.getFileBytes(CommonConfig.DOWNLOAD_PATH + "xjlt_ip_location_20220422.csv");
                if (ispBytes != null) {
                    ByteArrayInputStream ispInputStream = new ByteArrayInputStream(ispBytes);
                    this.ispReader = new InputStreamReader(ispInputStream);
                }
                byte[] idcBytes = FileByteUtils.getFileBytes(CommonConfig.DOWNLOAD_PATH + "xjlt_idc_renter_20220422.csv");
                if (idcBytes != null) {
                    ByteArrayInputStream idcInputStream = new ByteArrayInputStream(idcBytes);
                    this.idcReader = new InputStreamReader(idcInputStream);
                }
                byte[] internalBytes = FileByteUtils.getFileBytes(CommonConfig.DOWNLOAD_PATH + "xjlt_idc_internal_ip_20220422.csv");
                if (internalBytes != null) {
                    ByteArrayInputStream internalInputStream = new ByteArrayInputStream(internalBytes);
                    this.internalReader = new InputStreamReader(internalInputStream);
                }
            }
            IdcRenterUtils.readServerCsv(idcReader);
            LinkUtils.readLinkCsv(linkReader);
            IspUtils.readServerCsv(ispReader);
            FcUtils.readCsv(websktReader);
            FcUtils.readIcpCsv(icpReader);
            DnsServerUtils.readServerCsv(dnsReader);
            IPUtils.readInternalIpCsv(internalReader);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    public void updateCsv(List<CustomFile> customFiles) {
        try {
            ByteArrayInputStream websktInputStream = null;
            ByteArrayInputStream dnsInputStream = null;
            ByteArrayInputStream icpInputStream = null;
            ByteArrayInputStream linkInputStream = null;
            ByteArrayInputStream ispInputStream = null;
            ByteArrayInputStream idcInputStream = null;
            ByteArrayInputStream internalInputStream = null;
            for (CustomFile customFile : customFiles) {
                if ("websktDB_full-20211123-101740.csv".equals(customFile.getFileName()) && customFile.getContent() !=null) {
                    websktInputStream = new ByteArrayInputStream(customFile.getContent());
                    this.websktReader = new InputStreamReader(websktInputStream);
                    FcUtils.readCsv(websktReader);
                }
                if ("webskt_default_dns_server_info_full_20220427.csv".equals(customFile.getFileName())&& customFile.getContent() !=null) {
                    dnsInputStream = new ByteArrayInputStream(customFile.getContent());
                    this.dnsReader = new InputStreamReader(dnsInputStream);
                    DnsServerUtils.readServerCsv(dnsReader);
                }

                if ("icp.csv".equals(customFile.getFileName())&& customFile.getContent() !=null ) {
                    icpInputStream = new ByteArrayInputStream(customFile.getContent());
                    this.icpReader = new InputStreamReader(icpInputStream);
                    FcUtils.readIcpCsv(icpReader);
                }
                if ("link_direction.csv".equals(customFile.getFileName())&& customFile.getContent() !=null ) {
                    linkInputStream = new ByteArrayInputStream(customFile.getContent());
                    this.linkReader = new InputStreamReader(linkInputStream);
                    LinkUtils.readLinkCsv(linkReader);
                }
                if ("xjlt_ip_location_20220422.csv".equals(customFile.getFileName())&& customFile.getContent() !=null ) {
                    ispInputStream = new ByteArrayInputStream(customFile.getContent());
                    this.ispReader = new InputStreamReader(ispInputStream);
                    IspUtils.readServerCsv(ispReader);
                }
                if ("xjlt_idc_renter_20220422.csv".equals(customFile.getFileName())&& customFile.getContent() !=null) {
                    idcInputStream = new ByteArrayInputStream(customFile.getContent());
                    this.idcReader = new InputStreamReader(idcInputStream);
                    IdcRenterUtils.readServerCsv(idcReader);
                }
                if ("xjlt_idc_internal_ip_20220422.csv".equals(customFile.getFileName())&& customFile.getContent() !=null) {
                    internalInputStream = new ByteArrayInputStream(customFile.getContent());
                    this.internalReader = new InputStreamReader(internalInputStream);
                    IPUtils.readInternalIpCsv(internalReader);
                }
            }
        } catch (Exception e) {
            System.out.println(e.toString());
        }

    }
}
