package com.zdjizhi.etl.utils;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.etl.utils.csv.HighCsvReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

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
            HashMap<Long, String> newLinkMap = new HashMap<>((int) (csvReader.getLineNumber() / 0.75F + 1.0F));
            HighCsvReader.CsvIterator iterator = csvReader.getIterator();
            while (iterator.hasNext()) {
                Map<String, String> line = iterator.next();
                try {
                    String peerCity = line.get("peer_city");
                    String linkIdStr = line.get("link_id");
                    long linkId = Long.parseLong("".equals(linkIdStr) ? "0" : linkIdStr);

                    newLinkMap.put(linkId, peerCity);
                } catch (Exception lineException) {
                    LOG.error("LinkUtils line: " + line.toString() + " parse error：" + lineException, lineException);
                }
            }
            linkMap = newLinkMap;
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }

    public static void setLinkMessage(CnRecordLog cnRecordLog) {
        try {
            if (cnRecordLog.getCommon_out_link_id() != null && linkMap.containsKey(cnRecordLog.getCommon_out_link_id())) {
                cnRecordLog.setOut_link_direction(linkMap.get(cnRecordLog.getCommon_out_link_id()));
            }
            if (cnRecordLog.getCommon_in_link_id() != null && linkMap.containsKey(cnRecordLog.getCommon_in_link_id())) {
                cnRecordLog.setIn_link_direction(linkMap.get(cnRecordLog.getCommon_in_link_id()));
            }
        } catch (Exception e) {
            LOG.error("ETL解析错误\nrecordLog:{} \n", cnRecordLog, e);
        }
    }

    public static void close() {
        linkMap.clear();
        linkMap = null;
    }
}
