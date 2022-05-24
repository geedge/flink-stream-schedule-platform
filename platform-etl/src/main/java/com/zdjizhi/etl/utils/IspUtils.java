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
public class IspUtils {
    private static Logger LOG = LoggerFactory.getLogger(IspUtils.class);
    static HashMap<String, LinkAndIspInfo> ispmap = readServerCsv(CommonConfig.ISP_PATH);

    private static HashMap<String, LinkAndIspInfo> readServerCsv(String path) {
        HashMap<String, LinkAndIspInfo> ismap = new HashMap<>();
        try {
            DataInputStream in = new DataInputStream(new FileInputStream(new File(path)));
            CSVReader csvReader = new CSVReader(new InputStreamReader(in, StandardCharsets.UTF_8), CSVParser.DEFAULT_SEPARATOR,
                    CSVParser.DEFAULT_QUOTE_CHARACTER, CSVParser.DEFAULT_ESCAPE_CHARACTER, 1);
            for (String[] strs : csvReader) {
                String key = strs[1];
                LinkAndIspInfo linkDirectionInfo = new LinkAndIspInfo();
                linkDirectionInfo.setRegion(strs[3]);
                ismap.put(key, linkDirectionInfo);
            }
            csvReader.close();
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        return ismap;
    }

    public static void getIspMessage(CnRecordLog cnRecordLog) {
        if (cnRecordLog.getCommon_client_ip().contains(".")){
            String[] clientIp = cnRecordLog.getCommon_client_ip().split("\\.");
            String clientIpC = clientIp[0] + "." + clientIp[1] + "." + clientIp[2] + ".0";
            if (ispmap.containsKey(clientIpC)) {
                cnRecordLog.setClient_country("China");
                cnRecordLog.setClient_province("Xinjiang");
                cnRecordLog.setClient_region(ispmap.get(clientIpC).getRegion());
            }
        }if (cnRecordLog.getCommon_server_ip().contains(".")){
            String[] serverIp = cnRecordLog.getCommon_server_ip().split("\\.");
            String serverIpC = serverIp[0] + "." + serverIp[1] + "." + serverIp[2] + ".0";
            if (ispmap.containsKey(serverIpC)) {
                cnRecordLog.setServer_country("China");
                cnRecordLog.setServer_province("Xinjiang");
                cnRecordLog.setServer_region(ispmap.get(serverIpC).getRegion());
            }
        }

    }


    public static void main(String[] args) {
        HashMap<String, LinkAndIspInfo> map = readServerCsv(CommonConfig.ISP_PATH);
//        HashMap<String, CnRecordLog> map = readIcpCsv(CommonConfig.ICP_PATH);
        Set<String> keySet = map.keySet();
        for (String key : keySet) {
            System.out.println(map.get(key).toString());
        }
        System.out.println(map.size());
    }
}
