package com.zdjizhi.etl.utils.fqdn;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.etl.utils.csv.HighCsvReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class IcpUtils {

    private static final Logger logger = LoggerFactory.getLogger(IcpUtils.class);

    private static HashMap<String, String> icpMap = new HashMap<>();

    public static void readIcpCsv(HighCsvReader csvReader) {
        try {
            HashMap<String, String> newIcpMap = new HashMap<>((int) (csvReader.getLineNumber() / 0.75F + 1.0F));
            HighCsvReader.CsvIterator iterator = csvReader.getIterator();
            while (iterator.hasNext()) {
                Map<String, String> line = iterator.next();
                try {
                    String fqdn = line.get("fqdn");
                    String icpCompanyName = line.get("icp_company_name");

                    newIcpMap.put(fqdn, icpCompanyName);
                } catch (Exception lineException) {
                    logger.error("FcUtils icp file line: " + line.toString() + " parse error：" + lineException.getMessage(), lineException);
                }
            }
            icpMap = newIcpMap;
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    public static void setIcpMessage(CnRecordLog entity) {
        try {
            entity.setDomain_icp_company_name(getIcpMessage(entity.getDomain()));
        } catch (Exception e) {
            logger.error("ETL解析错误\nrecordLog:{} \n", entity, e);
        }
    }

    private static String getIcpMessage(String domain) {
        if (domain == null || domain.length() == 0) {
            return null;
        }
        if (icpMap.containsKey(domain)) {
            return icpMap.get(domain);
        } else {
            int index = domain.indexOf(".") + 1;
            if (index > 0) {
                return getIcpMessage(domain.substring(index));
            } else {
                return null;
            }
        }
    }

    public static void close() {
        icpMap.clear();
        icpMap = null;
    }
}
