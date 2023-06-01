package com.zdjizhi.etl.utils;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.etl.common.AppSketchLabel;
import com.zdjizhi.etl.common.DomainWhois;
import com.zdjizhi.utils.JsonMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.Reader;
import java.util.*;

/**
 * @ Author pengfeixu
 * @ Date 2022/2/15
 * @ Description
 */
public class WhoisUtils {
    private static final Logger LOG = LoggerFactory.getLogger(AppUtils.class);

    private static HashMap<String, String> whoIsMap =new HashMap<>();

    public static void readCsv(Reader whoisReader) {
        try {
            CsvReaderUtils csvReader = new CsvReaderUtils(whoisReader);
            List<Map<String, String>> data = csvReader.readAll();
            HashMap<String, String> newWhoIsMap = new HashMap<>(data.size() * 2);
            for (Map<String, String> line : data) {
                try {
                    String fqdn = line.get("fqdn");
                    String whoisRegistrantOrg = line.get("whois_registrant_org");

                    newWhoIsMap.put(fqdn, whoisRegistrantOrg);
                } catch (Exception lineException) {
                    System.out.println("WhoisUtils line: " + line.toString() + " parse error：" + lineException);
                    lineException.printStackTrace(System.out);
                }
            }
            whoIsMap = newWhoIsMap;
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }


    public static void setWhoisSketchMessage(CnRecordLog entity) {
        try {
            String domain = entity.getDomain();
            entity.setDomain_whois_org(whoIsMap.getOrDefault(domain, ""));
        } catch (Exception e) {
            LOG.error("ETL解析错误\nrecordLog:{} \n", entity, e);
        }

    }

    public static void main(String[] args) throws Exception {

        Set<String> keySet = whoIsMap.keySet();
        for (String key : keySet){
            System.out.println(whoIsMap.get(key).toString());
        }
        System.out.println(whoIsMap.size());
    }

}
