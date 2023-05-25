package com.zdjizhi.etl.utils;

import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.etl.common.LinkAndIspInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * @ Author pengfeixu
 * @ Date 2022/3/30
 * @ Description
 */
public class LinkUtils {
    private static Logger LOG = LoggerFactory.getLogger(LinkUtils.class);
    private static HashMap<Long, LinkAndIspInfo> linkmap = new HashMap<>();

    public static void readLinkCsv(Reader linkReader) {

        try {
            CSVReader csvReader = new CSVReader(linkReader, CSVParser.DEFAULT_SEPARATOR,
                    CSVParser.DEFAULT_QUOTE_CHARACTER, CSVParser.DEFAULT_ESCAPE_CHARACTER, 1);
            List<String[]> lines = csvReader.readAll();
            HashMap<Long, LinkAndIspInfo> newLinkmap = new HashMap<>(lines.size() * 2);
            for (String[] strs : lines) {
                long key = Long.parseLong(strs[6]);
                LinkAndIspInfo linkDirectionInfo = new LinkAndIspInfo();
                linkDirectionInfo.setEgress_link_direction(strs[4]);
                linkDirectionInfo.setIngress_link_direction(strs[4]);
                linkDirectionInfo.setLink_id(Integer.parseInt(strs[6]));
                newLinkmap.put(key, linkDirectionInfo);
            }
            linkmap = newLinkmap;
            csvReader.close();
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }

    public static void setLinkMessage(CnRecordLog cnRecordLog) {
        try {
            if (!linkmap.containsKey(cnRecordLog.getCommon_egress_link_id()) || !linkmap.containsKey(cnRecordLog.getCommon_ingress_link_id())) {
                cnRecordLog.setCommon_egress_link_id(0);
                cnRecordLog.setCommon_ingress_link_id(0);
            }

            if (linkmap.containsKey(cnRecordLog.getCommon_egress_link_id())) {
                cnRecordLog.setEgress_link_direction(linkmap.get(cnRecordLog.getCommon_egress_link_id()).getEgress_link_direction());
            }
            if (linkmap.containsKey(cnRecordLog.getCommon_ingress_link_id())) {
                cnRecordLog.setIngress_link_direction(linkmap.get(cnRecordLog.getCommon_ingress_link_id()).getIngress_link_direction());
            }
        } catch (Exception e) {
            LOG.error("ETL解析错误\nrecordLog:{} \n", cnRecordLog, e);
        }
    }

    public static void main(String[] args) {
        HashMap<Long, LinkAndIspInfo> map = linkmap;
        Set<Long> keySet = map.keySet();
        for (long key : keySet) {
            System.out.println(map.get(key).toString());
        }
        System.out.println(map.size());
    }
}
