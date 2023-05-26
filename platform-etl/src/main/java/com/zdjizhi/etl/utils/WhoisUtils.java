package com.zdjizhi.etl.utils;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.etl.common.DomainWhois;
import com.zdjizhi.utils.JsonMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.util.*;

/**
 * @ Author pengfeixu
 * @ Date 2022/2/15
 * @ Description
 */
public class WhoisUtils {
    private static Logger LOG = LoggerFactory.getLogger(AppUtils.class);

    private static HashMap<String, DomainWhois> smap =new HashMap<>();

    public static void readJson(BufferedReader whoisBufferedReader) {
        try {
            String whoisInput = "";
            StringBuilder whoisBuilder = new StringBuilder();
            while ((whoisInput = whoisBufferedReader.readLine()) != null) {
                whoisBuilder.append(whoisInput);
            }
            List<HashMap<String, Object>>  jarray = JSON.parseArray(whoisBuilder.toString(), TypeReference.mapType(HashMap.class, String.class, Object.class));
            smap = new HashMap<>(jarray.size() * 2);
            for (HashMap<String, Object> obj : jarray) {
                DomainWhois richEntity = new DomainWhois();
                richEntity.setFqdn(obj.get("fqdn").toString());
                if (obj.containsKey("whois_registrant_org")){
                    richEntity.setWhoisRegistrantOrg(obj.get("whois_registrant_org").toString());
                }
                smap.put(richEntity.getFqdn(), richEntity);
            }
        } catch (Exception e) {
            LOG.error("transaction whois json error "+e.toString());
        }
    }


    public static void setWhoisSketchMessage(CnRecordLog entity) {
        try {
            String domain = entity.getDomain();
            if (smap.containsKey(domain)) {
                entity.setDomain_whois_org(smap.get(domain).getWhoisRegistrantOrg());
            }else{
                entity.setDomain_whois_org("");
            }
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
