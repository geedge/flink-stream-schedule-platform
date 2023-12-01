package com.zdjizhi.etl.utils.fqdn;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.etl.utils.csv.HighCsvReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @ Author pengfeixu
 * @ Date 2022/2/15
 * @ Description
 */
public class WhoisUtils {
    private static final Logger LOG = LoggerFactory.getLogger(WhoisUtils.class);

    private static HashMap<String, String> whoIsMap =new HashMap<>();

    public static void readCsv(HighCsvReader csvReader) {
        try {
            HashMap<String, String> newWhoIsMap = new HashMap<>(csvReader.getLineNumber() * 2);
            HighCsvReader.CsvIterator iterator = csvReader.getIterator();
            while (iterator.hasNext()) {
                Map<String, String> line = iterator.next();
                try {
                    String fqdn = line.get("fqdn");
                    String whoisRegistrantOrg = line.get("whois_registrant_org");

                    newWhoIsMap.put(fqdn, whoisRegistrantOrg);
                } catch (Exception lineException) {
                    LOG.error("WhoisUtils line: " + line.toString() + " parse error：" + lineException, lineException);
                }
            }
            whoIsMap = newWhoIsMap;
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }


    public static void setWhoisSketchMessage(CnRecordLog entity) {
        try {
            entity.setDomain_whois_org(getDomainWhoisSketchMessage(entity.getDomain()));
        } catch (Exception e) {
            LOG.error("ETL解析错误\nrecordLog:{} \n", entity, e);
        }
    }

    private static String getDomainWhoisSketchMessage(String domain) {
        if (domain == null || domain.length() == 0) {
            return null;
        }
        if (whoIsMap.containsKey(domain)) {
            return whoIsMap.get(domain);
        } else {
            int index = domain.indexOf(".") + 1;
            if (index > 0) {
                return getDomainWhoisSketchMessage(domain.substring(index));
            } else {
                return null;
            }
        }
    }

    public static void close() {
        whoIsMap.clear();
        whoIsMap = null;
    }
}
