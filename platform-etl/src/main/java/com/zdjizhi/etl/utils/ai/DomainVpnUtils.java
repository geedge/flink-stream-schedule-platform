package com.zdjizhi.etl.utils.ai;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.etl.utils.csv.HighCsvReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/1/15 17:43
 */
public class DomainVpnUtils {

    private static final Logger LOG = LoggerFactory.getLogger(DomainVpnUtils.class);
    private static Map<String, String> map = new HashMap<>();


    public static void readCsv(HighCsvReader csvReader) {
        try {
            Map<String, String> newMap = new HashMap<>((int) (csvReader.getLineNumber() / 0.75F + 1.0F));
            HighCsvReader.CsvIterator iterator = csvReader.getIterator();
            while (iterator.hasNext()) {
                Map<String, String> line = iterator.next();
                try {
                    String domain = line.get("domain");
                    String vpnServiceName = line.get("vpn_service_name");

                    newMap.put(domain, vpnServiceName);
                } catch (Exception lineException) {
                    LOG.error("IdcRenterUtils line: " + line.toString() + " parse error：" + lineException, lineException);
                }
            }
            map = newMap;
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }


    public static void setInfo(CnRecordLog cnRecordLog) {
        try {
            String domain = cnRecordLog.getDomain();
            cnRecordLog.putDomain_tag(getInfo(domain));
        } catch (Exception e) {
            LOG.error("ETL解析错误\nrecordLog:{} \n", cnRecordLog, e);
        }
    }

    private static String getInfo(String domain) {
        if (domain == null || domain.length() == 0) {
            return null;
        }
        if (map.containsKey(domain)) {
            return map.get(domain);
        } else {
            int index = domain.indexOf(".") + 1;
            if (index > 0) {
                return getInfo(domain.substring(index));
            } else {
                return null;
            }
        }
    }
}
