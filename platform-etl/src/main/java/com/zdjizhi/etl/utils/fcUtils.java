package com.zdjizhi.etl.utils;

import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.common.Entity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

public class fcUtils {
    private static Logger LOG = LoggerFactory.getLogger(fcUtils.class);

    private static HashMap<String, CnRecordLog> smap = readCsv(CommonConfig.WEBSKT_PATH);
    private static HashMap<String, CnRecordLog> icpmap = readIcpCsv(CommonConfig.ICP_PATH);


    //todo 获取domain
    private static HashMap<String, CnRecordLog> readCsv(String path) {
        HashMap<String , CnRecordLog> fcmap =new HashMap<>();
        try {
            DataInputStream in = new DataInputStream(new FileInputStream(new File(path)));
            CSVReader csvReader = new CSVReader(new InputStreamReader(in, StandardCharsets.UTF_8), CSVParser.DEFAULT_SEPARATOR,
                    CSVParser.DEFAULT_QUOTE_CHARACTER, CSVParser.DEFAULT_ESCAPE_CHARACTER, 1);
            for (String[] strs : csvReader) {
                String key = strs[0];
                if (!key.startsWith("#")){
                    CnRecordLog catalog = new CnRecordLog();
                    catalog.setDomain(key);
                    catalog.setDomain_category_name(strs[6]);
                    catalog.setDomain_category_group(strs[7]);
                    catalog.setDomain_reputation_level(strs[4]);
                    catalog.setDomain_reputation_score(Long.parseLong(strs[3]));
//                    catalog.setDomain_icp_company_name(strs[5]);
                    fcmap.put(key, catalog);
                }
            }
            csvReader.close();
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        return  fcmap;
    }
    //todo 获取icp.csv
    private static HashMap<String, CnRecordLog> readIcpCsv(String path) {
        HashMap<String , CnRecordLog> icpmap =new HashMap<>();
        try {
            DataInputStream in = new DataInputStream(new FileInputStream(new File(path)));
            CSVReader csvReader = new CSVReader(new InputStreamReader(in, StandardCharsets.UTF_8), CSVParser.DEFAULT_SEPARATOR,
                    CSVParser.DEFAULT_QUOTE_CHARACTER, CSVParser.DEFAULT_ESCAPE_CHARACTER, 1);

            for (String[] strs : csvReader) {
                String key = strs[0];
                if (!key.startsWith("#")){
                    CnRecordLog catalog = new CnRecordLog();
                    catalog.setDomain(key);
                    catalog.setDomain_icp_company_name(strs[5]);
                    icpmap.put(key, catalog);

                }
            }
            csvReader.close();
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        return  icpmap;
    }
    //todo domain
    public static void getWebSketchMessage(CnRecordLog entity) {
        try {
            if(smap.containsKey(entity.getDomain())){
                entity.setDomain_reputation_score(smap.get(entity.getDomain()).getDomain_reputation_score());
                entity.setDomain_category_group(smap.get(entity.getDomain()).getDomain_category_group());
                entity.setDomain_category_name(smap.get(entity.getDomain()).getDomain_category_name());
                entity.setDomain_reputation_level(smap.get(entity.getDomain()).getDomain_reputation_level());
            }
        } catch (Exception e) {
            LOG.error(e.toString());
        }
    }

    //todo icp
    public static void getIcpMessage(CnRecordLog entity) {
        try {
            if(icpmap.containsKey(entity.getDomain())){
                entity.setDomain_icp_company_name(icpmap.get(entity.getDomain()).getDomain_icp_company_name());
            }
        } catch (Exception e) {
            LOG.error(e.toString());
        }
    }


    public static void main(String[] args) throws Exception {
        HashMap<String, CnRecordLog> map = readCsv(CommonConfig.WEBSKT_PATH);
//        HashMap<String, CnRecordLog> map = readIcpCsv(CommonConfig.ICP_PATH);
        Set<String> keySet = map.keySet();
        for (String key : keySet){
            System.out.println(map.get(key).toString());
        }
        System.out.println(map.size());
    }
}