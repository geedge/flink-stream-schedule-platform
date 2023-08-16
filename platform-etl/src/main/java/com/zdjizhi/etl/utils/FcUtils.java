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

    public static void readCsv(HighCsvReader csvReader) {
        try {
            HashMap<String, DomainCategoryReputation> newFqdnMap = new HashMap<>(csvReader.getLineNumber() * 2);
            HighCsvReader.CsvIterator iterator = csvReader.getIterator();
            while (iterator.hasNext()) {
                Map<String, String> line = iterator.next();
                try {
                    String fqdn = line.get("fqdn");
                    String reputationLevel = line.get("reputation_level");;
                    String categoryName = line.get("category_name");;
                    String categoryGroup = line.get("category_group");

                    DomainCategoryReputation catalog = new DomainCategoryReputation();
                    catalog.setFqdn(fqdn);
                    catalog.setCategoryName(categoryName);
                    catalog.setCategoryGroup(categoryGroup);
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

    public static void readIcpCsv(HighCsvReader csvReader) {
        try {
            HashMap<String, String> newIcpMap = new HashMap<>(csvReader.getLineNumber() * 2);
            HighCsvReader.CsvIterator iterator = csvReader.getIterator();
            while (iterator.hasNext()) {
                Map<String, String> line = iterator.next();
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
            DomainCategoryReputation domainCategoryReputation = getDomainWebSketchMessage(entity.getDomain());
            if(domainCategoryReputation != null){
                entity.setDomain_category_group(domainCategoryReputation.getCategoryGroup());
                entity.setDomain_category_name(domainCategoryReputation.getCategoryName());
                entity.setDomain_reputation_level(domainCategoryReputation.getReputationLevel());
            }
        } catch (Exception e) {
            LOG.error("ETL解析错误\nrecordLog:{} \n", entity, e);
        }
    }

    private static DomainCategoryReputation getDomainWebSketchMessage(String domain) {
        if (domain == null || domain.length() == 0) {
            return null;
        }
        if (fqdnMap.containsKey(domain)) {
            return fqdnMap.get(domain);
        } else {
            int index = domain.indexOf(".") + 1;
            if (index > 0) {
                return getDomainWebSketchMessage(domain.substring(index));
            } else {
                return null;
            }
        }
    }

    public static void setIcpMessage(CnRecordLog entity) {
        try {
            entity.setDomain_icp_company_name(getDomainIcpMessage(entity.getDomain()));
        } catch (Exception e) {
            LOG.error("ETL解析错误\nrecordLog:{} \n", entity, e);
        }
    }

    private static String getDomainIcpMessage(String domain) {
        if (domain == null || domain.length() == 0) {
            return null;
        }
        if (icpMap.containsKey(domain)) {
            return icpMap.get(domain);
        } else {
            int index = domain.indexOf(".") + 1;
            if (index > 0) {
                return getDomainIcpMessage(domain.substring(index));
            } else {
                return null;
            }
        }
    }
}