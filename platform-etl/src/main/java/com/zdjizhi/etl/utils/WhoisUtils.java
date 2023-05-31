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
    private static Logger LOG = LoggerFactory.getLogger(AppUtils.class);

    private static HashMap<String, String> smap =new HashMap<>();

    public static void readCsv(Reader whoisReader) {
        try {
            CSVReader csvReader = new CSVReader(whoisReader, CSVParser.DEFAULT_SEPARATOR,
                    CSVParser.DEFAULT_QUOTE_CHARACTER, CSVParser.DEFAULT_ESCAPE_CHARACTER, 1);
            List<String[]> lines = csvReader.readAll();
            HashMap<String, String> newSmap = new HashMap<>(lines.size() * 2);
            for (String[] line : lines) {
                try {
                    String fqdn = line[1];
                    String whoisRegistrantOrg = line[13];

                    newSmap.put(fqdn, whoisRegistrantOrg);
                } catch (Exception lineException) {
                    System.out.println("WhoisUtils line: " + Arrays.toString(line) + " 解析异常：" + lineException.getMessage());
                }
            }
            smap = newSmap;
            csvReader.close();
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }


    public static void setWhoisSketchMessage(CnRecordLog entity) {
        try {
            String domain = entity.getDomain();
            entity.setDomain_whois_org(smap.getOrDefault(domain, ""));
        } catch (Exception e) {
            LOG.error("ETL解析错误\nrecordLog:{} \n", entity, e);
        }

    }

    public static void main(String[] args) throws Exception {

        Set<String> keySet = smap.keySet();
        for (String key : keySet){
            System.out.println(smap.get(key).toString());
        }
        System.out.println(smap.size());
    }

}
