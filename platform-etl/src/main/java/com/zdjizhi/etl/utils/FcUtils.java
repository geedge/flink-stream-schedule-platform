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
import java.util.HashMap;
import java.util.Set;

public class FcUtils {
    private static Logger LOG = LoggerFactory.getLogger(FcUtils.class);
    private static HashMap<String, DomainCategoryReputation> smap = new HashMap<>();
    private static HashMap<String, DomainIcp> icpmap = new HashMap<>();

    public static void readCsv(Reader websktReader) {
        try {
//            smap.clear();
//            DataInputStream in = new DataInputStream(new FileInputStream(new File(path)));
            CSVReader csvReader = new CSVReader(websktReader, CSVParser.DEFAULT_SEPARATOR,
                    CSVParser.DEFAULT_QUOTE_CHARACTER, CSVParser.DEFAULT_ESCAPE_CHARACTER, 1);
            for (String[] strs : csvReader) {
                String key = strs[0];
                if (!key.startsWith("#")){
                    DomainCategoryReputation catalog = new DomainCategoryReputation();
                    catalog.setFqdn(key);
                    catalog.setCategoryName(strs[6]);
                    catalog.setCategoryGroup(strs[7]);
                    catalog.setReputationLevel(strs[4]);
                    catalog.setReputationScore(Long.parseLong(strs[3]));
                    smap.put(key, catalog);
                }
            }
            csvReader.close();
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
//        return  fcmap;
    }

    public static void readIcpCsv(Reader icpReader) {
//        HashMap<String , DomainIcp> icpmap =new HashMap<>();
        try {
//            icpmap.clear();
//            DataInputStream in = new DataInputStream(new FileInputStream(new File(path)));
            CSVReader csvReader = new CSVReader(icpReader, CSVParser.DEFAULT_SEPARATOR,
                    CSVParser.DEFAULT_QUOTE_CHARACTER, CSVParser.DEFAULT_ESCAPE_CHARACTER, 1);

            for (String[] strs : csvReader) {
                String key = strs[0];
                if (!key.startsWith("#")){
                    DomainIcp catalog = new DomainIcp();
                    catalog.setFqdn(key);
                    catalog.setIcpCompanyName(strs[5]);
                    icpmap.put(key, catalog);

                }
            }
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
                entity.setDomain_icp_company_name(icpmap.get(domain).getIcpCompanyName());
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