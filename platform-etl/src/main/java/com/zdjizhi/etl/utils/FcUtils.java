package com.zdjizhi.etl.utils;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.etl.common.DomainCategoryReputation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FcUtils {
    private static final Logger LOG = LoggerFactory.getLogger(FcUtils.class);
    private static HashMap<String, DomainCategoryReputation> fqdnMap = new HashMap<>();
    private static HashMap<String, String> icpMap = new HashMap<>();

    public static void readCsv(Reader websktReader) {
        try {
            List<String> needColumns = new ArrayList<>();
            needColumns.add("fqdn");
            needColumns.add("reputation_score");
            needColumns.add("reputation_level");
            needColumns.add("category_name");
            needColumns.add("category_group");
            CsvReaderUtils csvReader = new CsvReaderUtils(websktReader, needColumns);
            List<Map<String, String>> data = csvReader.readAll();
            HashMap<String, DomainCategoryReputation> newFqdnMap = new HashMap<>(data.size() * 2);
            for (Map<String, String> line : data) {
                try {
                    String fqdn = line.get("fqdn");
                    String reputationScoreStr = line.get("reputation_score");
                    long reputationScore = Long.parseLong("".equals(reputationScoreStr) ? "0" : reputationScoreStr);
                    String reputationLevel = line.get("reputation_level");;
                    String categoryName = line.get("category_name");;
                    String categoryGroup = line.get("category_group");

                    DomainCategoryReputation catalog = new DomainCategoryReputation();
                    catalog.setFqdn(fqdn);
                    catalog.setCategoryName(categoryName);
                    catalog.setCategoryGroup(categoryGroup);
                    catalog.setReputationScore(reputationScore);
                    catalog.setReputationLevel(reputationLevel);

                    newFqdnMap.put(fqdn, catalog);
                } catch (Exception lineException) {
                    System.out.println("FcUtils fqdn file line: " + line.toString() + " parse error：" + lineException);
                    lineException.printStackTrace(System.out);
                }
            }
            fqdnMap = newFqdnMap;
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }

    public static void readIcpCsv(Reader icpReader) {
        try {
            CsvReaderUtils csvReader = new CsvReaderUtils(icpReader);
            List<Map<String, String>> data = csvReader.readAll();
            HashMap<String, String> newIcpMap = new HashMap<>(data.size() * 2);
            for (Map<String, String> line : data) {
                try {
                    String fqdn = line.get("fqdn");
                    String icpCompanyName = line.get("icp_company_name");

                    newIcpMap.put(fqdn, icpCompanyName);
                } catch (Exception lineException) {
                    System.out.println("FcUtils icp file line: " + line.toString() + " parse error：" + lineException.getMessage());
                }
            }
            icpMap = newIcpMap;
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }

    public static void setWebSketchMessage(CnRecordLog entity) {
        try {
            String domain = entity.getDomain();
            if(fqdnMap.containsKey(domain)){
                DomainCategoryReputation domainCategoryReputation = fqdnMap.get(domain);
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
            if(icpMap.containsKey(domain)){
                entity.setDomain_icp_company_name(icpMap.get(domain));
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