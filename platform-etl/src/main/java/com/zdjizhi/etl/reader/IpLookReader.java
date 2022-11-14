package com.zdjizhi.etl.reader;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.maxmind.db.CHMCache;
import com.maxmind.db.Reader;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.common.CustomFile;
import com.zdjizhi.base.utils.FileByteUtils;
import com.zdjizhi.base.utils.HdfsUtils;
import com.zdjizhi.utils.IPUtil;
import com.zdjizhi.utils.StringUtil;
import org.apache.kafka.common.protocol.types.Field;
import scala.xml.pull.ExceptionEvent;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

public class IpLookReader {
    private String clusterOrSingle;
    private static final String UNKNOWN = "";

    private static final Log logger = LogFactory.get();
    private Reader ipv4Reader = null;
    private Reader asnv4Reader = null;
    private Reader asnv6Reader = null;

    public IpLookReader(String clusterOrSingle) {
        this.clusterOrSingle = clusterOrSingle;
    }


    public void loadIpLook(){
        try {
            if ("CLUSTER".equals(clusterOrSingle)) {
                byte[] ipv4Bytes = HdfsUtils.getFileBytes(CommonConfig.HDFS_PATH + "ip_v4.mmdb");
                if (ipv4Bytes!=null){
                    InputStream ipv4InputStream = new ByteArrayInputStream(ipv4Bytes);
                    this.ipv4Reader=new Reader(ipv4InputStream,new CHMCache());
                }

                byte[] asnv4Bytes = HdfsUtils.getFileBytes(CommonConfig.HDFS_PATH + "asn_v4.mmdb");
                if (asnv4Bytes!=null){
                    InputStream asnv4InputStream = new ByteArrayInputStream(asnv4Bytes);
                    this.asnv4Reader=new Reader(asnv4InputStream,new CHMCache());
                }

                byte[] asnv6Bytes = HdfsUtils.getFileBytes(CommonConfig.HDFS_PATH + "asn_v6.mmdb");
                if (asnv6Bytes!=null){
                    InputStream asnv6InputStream = new ByteArrayInputStream(asnv6Bytes);
                    this.asnv6Reader=new Reader(asnv6InputStream,new CHMCache());
                }
            }else if ("SINGLE".equals(clusterOrSingle)){
                byte[] ipv4Bytes = FileByteUtils.getFileBytes(CommonConfig.DOWNLOAD_PATH + "ip_v4.mmdb");
                if (ipv4Bytes!=null){
                    InputStream ipv4InputStream = new ByteArrayInputStream(ipv4Bytes);
                    this.ipv4Reader=new Reader(ipv4InputStream,new CHMCache());
                }

                byte[] asnv4Bytes = FileByteUtils.getFileBytes(CommonConfig.DOWNLOAD_PATH + "asn_v4.mmdb");
                if (asnv4Bytes!=null){
                    InputStream asnv4InputStream = new ByteArrayInputStream(asnv4Bytes);
                    this.asnv4Reader=new Reader(asnv4InputStream,new CHMCache());
                }

                byte[] asnv6Bytes = FileByteUtils.getFileBytes(CommonConfig.DOWNLOAD_PATH + "asn_v6.mmdb");
                if (asnv6Bytes!=null){
                    InputStream asnv6InputStream = new ByteArrayInputStream(asnv6Bytes);
                    this.asnv6Reader=new Reader(asnv6InputStream,new CHMCache());
                    System.out.println(asnv6Reader);
                }
            }
        }catch (Exception e){

        }
    }

    public JsonNode ipLookDetail(String ip){
        JsonNode ipv4 = null;
        try {
            InetAddress ipAddress = InetAddress.getByName(ip);
            ipv4 = ipv4Reader.get(ipAddress);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }catch (NullPointerException e){
            e.printStackTrace();
        }
        return ipv4;
    }

    /**
     * 获取ASN信息
     *
     * @param ip ip地址
     * @return asn信息
     */
//    String asnLookup(String ip) {
////        JsonNode location = getAsn(ip);
//        if (IPUtil.internalIp(ip) || StringUtil.isEmpty(location)) {
//            return UNKNOWN;
//        } else {
//            return StringUtil.setDefaultIfEmpty(location.get("ASN"), UNKNOWN).toString().replace("\"", "");
//        }
//    }


    public JsonNode getAsn(String ip) {
        String asn = "";
        JsonNode jsonNode = null;
        try {
            InetAddress ipAddress = InetAddress.getByName(ip);
            if (IPUtil.isIPAddress(ip)) {
                if (IPUtil.isIPv4Address(ip)) {
                    jsonNode = asnv4Reader.get(ipAddress);
                } else if (IPUtil.isIPv6Address(ip)) {
                    if (!StringUtil.isEmpty(asnv6Reader)){
                        jsonNode = asnv6Reader.get(ipAddress);
                    }
                }
            }
        } catch (RuntimeException e) {
            logger.error("IP address :" + ip + ", parser error " + e);
        } catch (IOException e) {
            logger.error("IP address " + ip + "of a host could not be determined, parser error " + e);
        }
        return jsonNode;
    }

    public void updateIpLook(List<CustomFile> customFiles){
        try{
            InputStream ipv4InputStream = null;
            InputStream asnv4InputStream = null;
            InputStream asnv6InputStream = null;
            for (CustomFile customFile : customFiles) {
                if ("ip_v4.mmdb".equals(customFile.getFileName())) {
                    ipv4InputStream = new ByteArrayInputStream(customFile.getContent());
                    this.ipv4Reader = new Reader(ipv4InputStream, new CHMCache());
                }
                if ("asn_v4.mmdb".equals(customFile.getFileName())) {
                    asnv4InputStream = new ByteArrayInputStream(customFile.getContent());
                    this.asnv4Reader=new Reader(asnv4InputStream,new CHMCache());
                }
                if ("asn_v6.mmdb".equals(customFile.getFileName())) {
                    asnv6InputStream = new ByteArrayInputStream(customFile.getContent());
                    this.asnv6Reader=new Reader(asnv6InputStream,new CHMCache());
                }
            }
        }catch (Exception e){

        }


    }




}
