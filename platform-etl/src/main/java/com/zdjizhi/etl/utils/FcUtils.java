package com.zdjizhi.etl.utils;

import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.etl.common.DomainCategoryReputation;
import com.zdjizhi.etl.common.DomainIcp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Set;

public class FcUtils {
    private static Logger LOG = LoggerFactory.getLogger(FcUtils.class);

    private static HashMap<String, DomainCategoryReputation> smap = readCsv(CommonConfig.WEBSKT_PATH);
    private static HashMap<String, DomainIcp> icpmap = readIcpCsv(CommonConfig.ICP_PATH);

    private static HashMap<String, DomainCategoryReputation> readCsv(String path) {
        HashMap<String , DomainCategoryReputation> fcmap =new HashMap<>();
        try {
            DataInputStream in = new DataInputStream(new FileInputStream(new File(path)));
            CSVReader csvReader = new CSVReader(new InputStreamReader(in, StandardCharsets.UTF_8), CSVParser.DEFAULT_SEPARATOR,
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
                    fcmap.put(key, catalog);
                }
            }
            csvReader.close();
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        return  fcmap;
    }

    private static HashMap<String, DomainIcp> readIcpCsv(String path) {
        HashMap<String , DomainIcp> icpmap =new HashMap<>();
        try {
            DataInputStream in = new DataInputStream(new FileInputStream(new File(path)));
            CSVReader csvReader = new CSVReader(new InputStreamReader(in, StandardCharsets.UTF_8), CSVParser.DEFAULT_SEPARATOR,
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
        return  icpmap;
    }

    public static void getWebSketchMessage(CnRecordLog entity) {
        String domain = entity.getDomain();
        if(smap.containsKey(domain)){
            DomainCategoryReputation domainCategoryReputation = smap.get(domain);
            entity.setDomain_reputation_score(domainCategoryReputation.getReputationScore());
            entity.setDomain_category_group(domainCategoryReputation.getCategoryGroup());
            entity.setDomain_category_name(domainCategoryReputation.getCategoryName());
            entity.setDomain_reputation_level(domainCategoryReputation.getReputationLevel());
        }
    }

    public static void getIcpMessage(CnRecordLog entity) {
        String domain = entity.getDomain();
        if(icpmap.containsKey(domain)){
            entity.setDomain_icp_company_name(icpmap.get(domain).getIcpCompanyName());
        }
    }


    public static void main(String[] args) throws Exception {
        HashMap<String, DomainCategoryReputation> map = readCsv(CommonConfig.WEBSKT_PATH);
//        HashMap<String, CnRecordLog> map = readIcpCsv(CommonConfig.ICP_PATH);
        Set<String> keySet = map.keySet();
        for (String key : keySet){
            System.out.println(map.get(key).toString());
        }
        System.out.println(map.size());
    }
}