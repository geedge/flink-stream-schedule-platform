package com.zdjizhi.etl.utils.tag;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.etl.utils.csv.HighCsvReader;
import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import org.apache.flink.shaded.guava18.com.google.common.collect.Range;
import org.apache.flink.shaded.guava18.com.google.common.collect.TreeRangeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class IpKnowledge extends AbstractKnowledge {

    private static final Logger logger = LoggerFactory.getLogger(IpKnowledge.class);

    private TreeRangeMap<IPAddress, Node> ipTagRules = TreeRangeMap.create();

    @Override
    public CustomKnowledgeUtils.LibraryType getType() {
        return CustomKnowledgeUtils.LibraryType.IP;
    }

    @Override
    public void parseContents(Map<String, byte[]> contents) {
        TreeRangeMap<IPAddress, Node> newIpTagRules = TreeRangeMap.create();
        for (String s : contents.keySet()) {
            parseIpTagRules(newIpTagRules, convertId(s), contents.get(s));
        }
        ipTagRules = newIpTagRules;
    }

    @Override
    public void setTag(CnRecordLog entity) {
        try {
            Node clientIpNode = getIpNode(entity.getCommon_client_ip());
            entity.putClient_ip_tags(clientIpNode.getTags());
            //client ip not need detection
            /*BaseTagUtils.setRuleIdAndIocType(entity, clientIpNode);*/
            Node serverIpNode = getIpNode(entity.getCommon_server_ip());
            entity.putServer_ip_tags(serverIpNode.getTags());
            CustomKnowledgeUtils.setRuleIdAndIocType(entity, serverIpNode);
        } catch (Exception e) {
            logger.error("ETL解析错误\nrecordLog:{} \n", entity, e);
        }
    }

    private Node getIpNode(String ip) {
        Node node = null;
        IPAddress address = new IPAddressString(ip).getAddress();
        if (address != null) {
            node = ipTagRules.get(address);
        }
        return node == null ? new Node() : node;
    }

    private void parseIpTagRules(TreeRangeMap<IPAddress, Node> treeRangeMap, Long id, byte[] ipFileContent) {
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

                    IPAddress startIpAddress;
                    IPAddress endIpAddress;
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
                        logger.warn("unknown addrFormat: " + addrFormat);
                        continue;
                    }

                    Map<Range<IPAddress>, Node> rangeListMap = treeRangeMap.subRangeMap(Range.closed(startIpAddress, endIpAddress)).asMapOfRanges();
                    TreeRangeMap<IPAddress, Node> subRangeMap = TreeRangeMap.create();
                    Node node = new Node();
                    node.add(tagValue, id);
                    subRangeMap.put(Range.closed(startIpAddress, endIpAddress), node);
                    rangeListMap.forEach((ipAddressRange, ipAddressNode) -> {
                        ipAddressNode.add(tagValue, id);
                        subRangeMap.put(ipAddressRange, ipAddressNode);
                    });
                    treeRangeMap.putAll(subRangeMap);
                } catch (Exception lineException) {
                    logger.error("ip tag line: " + line.toString() + " parse error：" + lineException, lineException);
                }
            }
        } catch (Exception e) {
            logger.error("parse ip rules id" + id + " failed", e);
        }
    }
}
