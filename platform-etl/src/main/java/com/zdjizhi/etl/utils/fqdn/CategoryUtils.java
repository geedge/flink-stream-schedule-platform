package com.zdjizhi.etl.utils.fqdn;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.etl.common.DomainCategoryReputation;
import com.zdjizhi.etl.utils.csv.HighCsvReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class CategoryUtils {

    private static final Logger logger = LoggerFactory.getLogger(CategoryUtils.class);

    private static HashMap<String, DomainCategoryReputation> categoryMap = new HashMap<>();

    public static void readCategoryCsv(HighCsvReader csvReader) {
        try {
            HashMap<String, DomainCategoryReputation> newCategoryMap = new HashMap<>((int) (csvReader.getLineNumber() / 0.75F + 1.0F));
            HighCsvReader.CsvIterator iterator = csvReader.getIterator();
            while (iterator.hasNext()) {
                Map<String, String> line = iterator.next();
                try {
                    String fqdn = line.get("fqdn");
                    String reputationLevel = line.get("reputation_level");
                    String categoryName = line.get("category_name");
                    String categoryGroup = line.get("category_group");

                    DomainCategoryReputation catalog = new DomainCategoryReputation();
                    catalog.setFqdn(fqdn);
                    catalog.setCategoryName(categoryName);
                    catalog.setCategoryGroup(categoryGroup);
                    catalog.setReputationLevel(reputationLevel);

                    newCategoryMap.put(fqdn, catalog);
                } catch (Exception lineException) {
                    logger.error("FcUtils fqdn file line: " + line.toString() + " parse error：" + lineException, lineException);
                }
            }
            categoryMap = newCategoryMap;
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    public static void setCategoryMessage(CnRecordLog entity) {
        try {
            DomainCategoryReputation domainCategoryReputation = getCategoryMessage(entity.getDomain());
            if (domainCategoryReputation != null) {
                entity.setDomain_category_group(domainCategoryReputation.getCategoryGroup());
                entity.setDomain_category_name(domainCategoryReputation.getCategoryName());
                entity.setDomain_reputation_level(domainCategoryReputation.getReputationLevel());
            }
        } catch (Exception e) {
            logger.error("ETL解析错误\nrecordLog:{} \n", entity, e);
        }
    }

    private static DomainCategoryReputation getCategoryMessage(String domain) {
        if (domain == null || domain.length() == 0) {
            return null;
        }
        if (categoryMap.containsKey(domain)) {
            return categoryMap.get(domain);
        } else {
            int index = domain.indexOf(".") + 1;
            if (index > 0) {
                return getCategoryMessage(domain.substring(index));
            } else {
                return null;
            }
        }
    }

    public static void close() {
        categoryMap.clear();
        categoryMap = null;
    }
}
