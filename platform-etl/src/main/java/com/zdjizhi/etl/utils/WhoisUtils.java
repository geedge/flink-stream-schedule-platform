package com.zdjizhi.etl.utils;

import com.fasterxml.jackson.databind.JavaType;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.etl.common.DomainWhois;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

/**
 * @ Author pengfeixu
 * @ Date 2022/2/15
 * @ Description
 */
public class WhoisUtils {
    private static Logger LOG = LoggerFactory.getLogger(AppUtils.class);
    private static JsonMapper jsonMapperInstance = JsonMapper.getInstance();
    private static JavaType listType = jsonMapperInstance.createCollectionType(ArrayList.class, HashMap.class);

    private static HashMap<String, DomainWhois> smap = readJson();

    private static HashMap<String, DomainWhois> readJson() {
        HashMap<String, DomainWhois> whoismap = new HashMap<>();
        try {
            String s = JsonUtils.readJsonFile(CommonConfig.WHOIS_PATH);
            ArrayList<HashMap<String, Object>>  jarray = jsonMapperInstance.fromJson(s, listType);
            for (HashMap<String, Object> obj : jarray) {
                DomainWhois richEntity = new DomainWhois();
                richEntity.setFqdn(obj.get("fqdn").toString());
                if (obj.containsKey("whois_registrant_org")){
                    richEntity.setWhoisRegistrantOrg(obj.get("whois_registrant_org").toString());
                }
                whoismap.put(richEntity.getFqdn(), richEntity);
            }
        } catch (Exception e) {
            LOG.error("transaction whois json error "+e.toString());
        }
        return whoismap;
    }


    public static void getWhoisSketchMessage(CnRecordLog entity) {
        String domain = entity.getDomain();
        if (smap.containsKey(domain)) {
            entity.setDomain_whois_org(smap.get(domain).getWhoisRegistrantOrg());
        }else{
            entity.setDomain_whois_org(" ");
        }
    }

    public static void main(String[] args) throws Exception {

        HashMap<String, DomainWhois> map = readJson();
        Set<String> keySet = map.keySet();
        for (String key : keySet){
            System.out.println(map.get(key).toString());
        }
        System.out.println(map.size());
    }

}
