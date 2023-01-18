package com.zdjizhi.security.operator;

import com.fasterxml.jackson.databind.JavaType;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.etl.utils.JsonUtils;
import com.zdjizhi.security.common.IocJsonLog;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;

public class IocSourceFunction implements SourceFunction<Tuple3<Tuple2<String,IocJsonLog>,Tuple2<String,IocJsonLog>,Tuple2<String,IocJsonLog>>> {

    private static Logger LOG = LoggerFactory.getLogger(IocSourceFunction.class);
    private static JsonMapper jsonMapperInstance = JsonMapper.getInstance();
    private static JavaType listType = jsonMapperInstance.createCollectionType(ArrayList.class, HashMap.class);

    @Override
    public void run(SourceContext<Tuple3<Tuple2<String,IocJsonLog>,Tuple2<String,IocJsonLog>,Tuple2<String,IocJsonLog>>> ctx) throws Exception {

        try {
            String iocJson = JsonUtils.readJsonFile(CommonConfig.IOC_PATH);
            ArrayList<HashMap<String, Object>> arr = jsonMapperInstance.fromJson(iocJson, listType);

            Tuple2<String, IocJsonLog> urlTuple = new Tuple2<>();
            Tuple2<String, IocJsonLog> domainTuple = new Tuple2<>();
            Tuple2<String, IocJsonLog> ipTuple = new Tuple2<>();

            for (HashMap<String, Object> obj : arr) {
                //字典对象
                String iocType = obj.get("ioc_type").toString();
                String iocValue = obj.get("ioc_value").toString();
                if (("url").equals(iocType)) {
                    IocJsonLog iocJsonLog = new IocJsonLog();
                    setCommonField(obj, iocJsonLog);
                    urlTuple = Tuple2.of(iocValue, iocJsonLog);
                }else if ("domain".equals(iocType)) {
                    IocJsonLog iocJsonLog = new IocJsonLog();
                    setCommonField(obj, iocJsonLog);
                    domainTuple = Tuple2.of(iocValue, iocJsonLog);
                }else if ("ip".equals(iocType)) {
                    IocJsonLog iocJsonLog = new IocJsonLog();
                    setCommonField(obj, iocJsonLog);
                    ipTuple = Tuple2.of(iocValue, iocJsonLog);
                }
                ctx.collect(Tuple3.of(ipTuple,domainTuple,urlTuple));
            }
        } catch (Exception e) {
            LOG.error("加载IOC库失败 \n", e);
        }
    }

    private void setCommonField(HashMap<String, Object> obj, IocJsonLog iocJsonLog) {
        iocJsonLog.setIoc_type(obj.get("ioc_type").toString());
        iocJsonLog.setIoc_value(obj.get("ioc_value").toString());
        iocJsonLog.setThreat_type(obj.get("threat_type").toString());
        iocJsonLog.setMalware_name(obj.get("malware_name").toString());
        //malware_alias
        if (StringUtil.isNotEmpty(obj.get("malware_alias").toString())) {
            iocJsonLog.setMalware_alias(obj.get("malware_alias").toString());
        } else {
            iocJsonLog.setMalware_alias(" ");
        }
        //mitre_attack_description
        if (StringUtil.isNotEmpty(obj.get("mitre_attack_description").toString())) {
            iocJsonLog.setMitre_attack_description(obj.get("mitre_attack_description").toString());
        } else {
            iocJsonLog.setMitre_attack_description(" ");
        }
        //mitre_attack_platforms
        if (StringUtil.isNotEmpty(obj.get("mitre_attack_platforms").toString())) {
            iocJsonLog.setMitre_attack_platforms(obj.get("mitre_attack_platforms").toString());
        } else {
            iocJsonLog.setMitre_attack_platforms(" ");
        }
        //mitre_attack_techniques
        if (StringUtil.isNotEmpty(obj.get("mitre_attack_techniques").toString())) {
            String mitreAttackTechniques = obj.get("mitre_attack_techniques").toString().replace("\"", "");
            iocJsonLog.setMitre_attack_techniques(mitreAttackTechniques);
        } else {
            iocJsonLog.setMitre_attack_techniques(" ");
        }
        //mitre_attack_groups
        if (StringUtil.isNotEmpty(obj.get("mitre_attack_groups").toString())) {
            iocJsonLog.setMitre_attack_groups(obj.get("mitre_attack_groups").toString().replace("\"", ""));
        } else {
            iocJsonLog.setMitre_attack_groups(" ");
        }
    }

    @Override
    public void cancel() {

    }
}
