package com.zdjizhi.etl.utils.tag;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.etl.utils.csv.HighCsvReader;
import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import org.apache.flink.shaded.guava18.com.google.common.collect.Range;
import org.apache.flink.shaded.guava18.com.google.common.collect.RangeMap;
import org.apache.flink.shaded.guava18.com.google.common.collect.TreeRangeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class IpTagUtils {

    private static final Logger logger = LoggerFactory.getLogger(IpTagUtils.class);

    private static TreeRangeMap<IPAddress, Node> ipTagRules = TreeRangeMap.create();

    public static void updateTag(Map<Long, byte[]> contents) {
        TreeRangeMap<IPAddress, Node> newIpTagRules = TreeRangeMap.create();
        for (Long s : contents.keySet()) {
            parseIpTagRules(newIpTagRules, s, contents.get(s));
        }
        ipTagRules = newIpTagRules;
    }

    public static void setTag(CnRecordLog entity) {
        try {
            Node clientIpNode = getIpNode(entity.getCommon_client_ip());
            entity.putClient_ip_tags(clientIpNode.getTags());
            //client ip not need detection
            /*BaseTagUtils.setRuleIdAndIocType(entity, clientIpNode);*/
            Node serverIpNode = getIpNode(entity.getCommon_server_ip());
            entity.putServer_ip_tags(serverIpNode.getTags());
            BaseTagUtils.setRuleIdAndIocType(entity, serverIpNode);
        } catch (Exception e) {
            logger.error("ETL解析错误\nrecordLog:{} \n", entity, e);
        }
    }

    private static Node getIpNode(String ip) {
        Node node = null;
        IPAddress address = new IPAddressString(ip).getAddress();
        if (address != null) {
            node = ipTagRules.get(address);
        }
        return node == null ? new Node() : node;
    }

    private static void parseIpTagRules(TreeRangeMap<IPAddress, Node> treeRangeMap, Long id, byte[] ipFileContent) {
        try {
            InputStreamReader reader = new InputStreamReader(new ByteArrayInputStream(ipFileContent));
            List<String> needColumns = new ArrayList<>();
            needColumns.add("tag_value");
            needColumns.add("addr_format");
            needColumns.add("ip1");
            needColumns.add("ip2");
            HighCsvReader csvReader = new HighCsvReader(reader, needColumns);
            HighCsvReader.CsvIterator iterator = csvReader.getIterator();
            while (iterator.hasNext()) {
                Map<String, String> line = iterator.next();
                try {
                    String tagValue = line.get("tag_value");
                    String addrFormat = line.get("addr_format");
                    String ip1 = line.get("ip1");
                    String ip2 = line.get("ip2");

                    IPAddress startIpAddress = null;
                    IPAddress endIpAddress = null;
                    if ("Single".equals(addrFormat)) {
                        IPAddress ipAddress = new IPAddressString(ip1).getAddress();
                        if (ipAddress == null) {
                            continue;
                        }
                        startIpAddress = ipAddress;
                        endIpAddress = ipAddress;
                    } else if ("Range".equals(addrFormat)) {
                        IPAddress ipAddress1 = new IPAddressString(ip1).getAddress();
                        IPAddress ipAddress2 = new IPAddressString(ip2).getAddress();
                        if (ipAddress1 == null || ipAddress2 == null) {
                            continue;
                        }
                        startIpAddress = ipAddress1;
                        endIpAddress = ipAddress2;
                    } else if ("CIDR".equals(addrFormat)) {
                        IPAddress cidrIpAddress = new IPAddressString(ip1 + "/" + ip2).getAddress();
                        if (cidrIpAddress == null) {
                            continue;
                        }
                        IPAddress ipAddressLower = cidrIpAddress.getLower();
                        IPAddress ipAddressUpper = cidrIpAddress.getUpper();
                        startIpAddress = ipAddressLower;
                        endIpAddress = ipAddressUpper;
                    } else {
                        System.out.println("unknown addrFormat: " + addrFormat);
                        continue;
                    }

                    RangeMap<IPAddress, Node> ipAddressListRangeMap = treeRangeMap.subRangeMap(Range.closed(startIpAddress, endIpAddress));
                    Map<Range<IPAddress>, Node> rangeListMap = ipAddressListRangeMap.asMapOfRanges();
                    TreeRangeMap<IPAddress, Node> subRangeMap = TreeRangeMap.create();
                    Node node = new Node();
                    node.getTags().add(tagValue);
                    node.put(id, "ip");
                    subRangeMap.put(Range.closed(startIpAddress, endIpAddress), node);
                    for (Range<IPAddress> ipAddressRange : rangeListMap.keySet()) {
                        Node ipAddressNode = ipAddressListRangeMap.get(ipAddressRange.lowerEndpoint());
                        ipAddressNode.getTags().add(tagValue);
                        node.put(id, "ip");
                        subRangeMap.put(ipAddressRange, ipAddressNode);
                    }
                    treeRangeMap.putAll(subRangeMap);
                } catch (Exception lineException) {
                    System.out.println("ip tag line: " + line.toString() + " parse error：" + lineException);
                    lineException.printStackTrace(System.out);
                }
            }
        } catch (Exception e) {
            logger.error("parse ip rules id" + id + " failed", e);
        }


    }
}
