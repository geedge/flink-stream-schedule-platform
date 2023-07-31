package com.zdjizhi.etl.utils;

import com.zdjizhi.base.common.CnRecordLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

/**
 * @ Author pengfeixu
 * @ Date 2022/3/30
 * @ Description
 */
public class LinkUtils {
    private static final Logger LOG = LoggerFactory.getLogger(LinkUtils.class);
    private static HashMap<Long, String> linkMap = new HashMap<>();

    public static void readLinkCsv(HighCsvReader csvReader) {
        try {
            HashMap<Long, String> newLinkMap = new HashMap<>(csvReader.getLineNumber() * 2);
            HighCsvReader.CsvIterator iterator = csvReader.getIterator();
            while (iterator.hasNext()) {
                Map<String, String> line = iterator.next();
                try {
                    String peerCity = line.get("peer_city");
                    String linkIdStr = line.get("link_id");
                    long linkId = Long.parseLong("".equals(linkIdStr) ? "0" : linkIdStr);

                    newLinkMap.put(linkId, peerCity);
                } catch (Exception lineException) {
                    System.out.println("LinkUtils line: " + line.toString() + " parse error：" + lineException);
                    lineException.printStackTrace(System.out);
                }
            }
            linkMap = newLinkMap;
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }

    public static void setLinkMessage(CnRecordLog cnRecordLog) {
        try {
            if (!linkMap.containsKey(cnRecordLog.getCommon_egress_link_id()) || !linkMap.containsKey(cnRecordLog.getCommon_ingress_link_id())) {
                cnRecordLog.setCommon_egress_link_id(0);
                cnRecordLog.setCommon_ingress_link_id(0);
            }

            if (linkMap.containsKey(cnRecordLog.getCommon_egress_link_id())) {
                cnRecordLog.setEgress_link_direction(linkMap.get(cnRecordLog.getCommon_egress_link_id()));
            }
            if (linkMap.containsKey(cnRecordLog.getCommon_ingress_link_id())) {
                cnRecordLog.setIngress_link_direction(linkMap.get(cnRecordLog.getCommon_ingress_link_id()));
            }
        } catch (Exception e) {
            LOG.error("ETL解析错误\nrecordLog:{} \n", cnRecordLog, e);
        }
    }
}
