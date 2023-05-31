package com.zdjizhi.etl.utils;

import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.etl.common.LinkAndIspInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;
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
    private static HashMap<Long, String> linkmap = new HashMap<>();

    public static void readLinkCsv(Reader linkReader) {
        try {
            CSVReader csvReader = new CSVReader(linkReader, CSVParser.DEFAULT_SEPARATOR,
                    CSVParser.DEFAULT_QUOTE_CHARACTER, CSVParser.DEFAULT_ESCAPE_CHARACTER, 1);
            List<String[]> lines = csvReader.readAll();
            HashMap<Long, String> newLinkmap = new HashMap<>(lines.size() * 2);
            for (String[] line : lines) {
                try {
                    String peerCity = line[4];
                    long linkId = Long.parseLong("".equals(line[5]) ? "0" : line[5]);

                    newLinkmap.put(linkId, peerCity);
                } catch (Exception lineException) {
                    System.out.println("LinkUtils line: " + Arrays.toString(line) + " 解析异常：" + lineException.getMessage());
                }
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
                cnRecordLog.setEgress_link_direction(linkmap.get(cnRecordLog.getCommon_egress_link_id()));
            }
            if (linkmap.containsKey(cnRecordLog.getCommon_ingress_link_id())) {
                cnRecordLog.setIngress_link_direction(linkmap.get(cnRecordLog.getCommon_ingress_link_id()));
            }
        } catch (Exception e) {
            LOG.error("ETL解析错误\nrecordLog:{} \n", cnRecordLog, e);
        }
    }
}
