package com.zdjizhi.etl.utils;

import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.common.Entity;
import com.zdjizhi.etl.common.LinkAndIspInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Set;

/**
 * @ Author pengfeixu
 * @ Date 2022/3/30
 * @ Description
 */
public class LinkUtils {
    private static Logger LOG = LoggerFactory.getLogger(LinkUtils.class);
    private static HashMap<Long, LinkAndIspInfo> linkmap = readLinkCsv(CommonConfig.LINK_PATH);

    private static HashMap<Long, LinkAndIspInfo> readLinkCsv(String path) {
        HashMap<Long, LinkAndIspInfo> linkDirectionmap = new HashMap<>();
        try {
            DataInputStream in = new DataInputStream(new FileInputStream(new File(path)));
            CSVReader csvReader = new CSVReader(new InputStreamReader(in, StandardCharsets.UTF_8), CSVParser.DEFAULT_SEPARATOR,
                    CSVParser.DEFAULT_QUOTE_CHARACTER, CSVParser.DEFAULT_ESCAPE_CHARACTER, 1);

            for (String[] strs : csvReader) {
                long key=Long.parseLong(strs[6]);
                    LinkAndIspInfo linkDirectionInfo = new LinkAndIspInfo();
                    linkDirectionInfo.setEgress_link_direction(strs[3]);
                    linkDirectionInfo.setIngress_link_direction(strs[3]);
                    linkDirectionmap.put(key, linkDirectionInfo);
            }
            csvReader.close();
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        return linkDirectionmap;
    }

    public static void getLinkMessage(Entity entity, CnRecordLog cnRecordLog) {
        if (linkmap.containsKey(entity.getCommon_egress_link_id())){
            cnRecordLog.setEgress_link_direction(linkmap.get(entity.getCommon_egress_link_id()).getEgress_link_direction());
        }
        if (linkmap.containsKey(entity.getCommon_ingress_link_id())){
            cnRecordLog.setIngress_link_direction(linkmap.get(entity.getCommon_ingress_link_id()).getIngress_link_direction());
        }
    }

    public static void main(String[] args) {
        HashMap<Long, LinkAndIspInfo> map = readLinkCsv(CommonConfig.LINK_PATH);
        Set<Long> keySet = map.keySet();
        for (long key : keySet){
            System.out.println(map.get(key).toString());
        }
        System.out.println(map.size());
    }
}
