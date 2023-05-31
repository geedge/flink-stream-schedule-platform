package com.zdjizhi.etl.utils;

import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.etl.common.DomainCategoryReputation;
import com.zdjizhi.etl.common.DomainIcp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class FcUtils {
    private static Logger LOG = LoggerFactory.getLogger(FcUtils.class);
    private static HashMap<String, DomainCategoryReputation> smap = new HashMap<>();
    private static HashMap<String, String> icpmap = new HashMap<>();

    public static void readCsv(Reader websktReader) {
        try {
            CSVReader csvReader = new CSVReader(websktReader, CSVParser.DEFAULT_SEPARATOR,
                    CSVParser.DEFAULT_QUOTE_CHARACTER, CSVParser.DEFAULT_ESCAPE_CHARACTER, 1);
            List<String[]> lines = csvReader.readAll();
            HashMap<String, DomainCategoryReputation> newSmap = new HashMap<>(lines.size() * 2);
            for (String[] line : lines) {
                try {
                    String fqdn = line[1];
                    long reputationScore = Long.parseLong("".equals(line[5]) ? "0" : line[5]);
                    String reputationLevel = line[6];
                    String categoryName = line[8];
                    String categoryGroup = line[9];

                    DomainCategoryReputation catalog = new DomainCategoryReputation();
                    catalog.setFqdn(fqdn);
                    catalog.setCategoryName(categoryName);
                    catalog.setCategoryGroup(categoryGroup);
                    catalog.setReputationScore(reputationScore);
                    catalog.setReputationLevel(reputationLevel);

                    newSmap.put(fqdn, catalog);
                } catch (Exception lineException) {
                    System.out.println("FcUtils weskt file line: " + Arrays.toString(line) + " 解析异常：" + lineException.getMessage());
                }
            }
            smap = newSmap;
            csvReader.close();
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }

    public static void readIcpCsv(Reader icpReader) {
        try {
            CSVReader csvReader = new CSVReader(icpReader, CSVParser.DEFAULT_SEPARATOR,
                    CSVParser.DEFAULT_QUOTE_CHARACTER, CSVParser.DEFAULT_ESCAPE_CHARACTER, 1);
            List<String[]> lines = csvReader.readAll();
            HashMap<String, String> newIcpmap = new HashMap<>(lines.size() * 2);
            for (String[] line : lines) {
                try {
                    String fqdn = line[1];
                    String icpCompanyName = line[8];

                    newIcpmap.put(fqdn, icpCompanyName);
                } catch (Exception lineException) {
                    System.out.println("FcUtils icp file line: " + Arrays.toString(line) + " 解析异常：" + lineException.getMessage());
                }
            }
            icpmap = newIcpmap;
            csvReader.close();
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }

    public static void setWebSketchMessage(CnRecordLog entity) {
        try {
            String domain = entity.getDomain();
            if(smap.containsKey(domain)){
                DomainCategoryReputation domainCategoryReputation = smap.get(domain);
                entity.setDomain_reputation_score(domainCategoryReputation.getReputationScore());
                entity.setDomain_category_group(domainCategoryReputation.getCategoryGroup());
                entity.setDomain_category_name(domainCategoryReputation.getCategoryName());
                entity.setDomain_reputation_level(domainCategoryReputation.getReputationLevel());
            }
        } catch (Exception e) {
            LOG.error("ETL解析错误\nrecordLog:{} \n", entity, e);
        }
    }

    public static void setIcpMessage(CnRecordLog entity) {
        try {
            String domain = entity.getDomain();
            if(icpmap.containsKey(domain)){
                entity.setDomain_icp_company_name(icpmap.get(domain));
            }
        } catch (Exception e) {
            LOG.error("ETL解析错误\nrecordLog:{} \n", entity, e);
        }

    }


//    public static void main(String[] args) throws Exception {
////        HashMap<String, DomainCategoryReputation> map = readCsv(CommonConfig.WEBSKT_PATH);
////        HashMap<String, CnRecordLog> map = readIcpCsv(CommonConfig.ICP_PATH);
//        Set<String> keySet = smap.keySet();
//        for (String key : keySet){
//            System.out.println(smap.get(key).toString());
//        }
//        System.out.println(smap.size());
//
//        Set<String> keySet1 = icpmap.keySet();
//        for (String key : keySet){
//            System.out.println(icpmap.get(key).toString());
//        }
//        System.out.println(icpmap.size());
//    }
}