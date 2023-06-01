package com.zdjizhi.security.operator;

import com.zdjizhi.base.common.CommonInternalConfig;
import com.zdjizhi.base.utils.FileUtils;
import com.zdjizhi.etl.utils.CsvReaderUtils;
import com.zdjizhi.security.common.IocJsonLog;
import com.zdjizhi.utils.StringUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;

public class IocSourceFunction implements SourceFunction<Tuple3<Tuple2<String,IocJsonLog>,Tuple2<String,IocJsonLog>,Tuple2<String,IocJsonLog>>> {

    private static final Logger LOG = LoggerFactory.getLogger(IocSourceFunction.class);

    @Override
    public void run(SourceContext<Tuple3<Tuple2<String,IocJsonLog>,Tuple2<String,IocJsonLog>,Tuple2<String,IocJsonLog>>> ctx) throws Exception {

        try {
            byte[] bytes = FileUtils.getFileBytes(CommonInternalConfig.IOC_TYPE + ".csv");
            InputStreamReader reader = new InputStreamReader(new ByteArrayInputStream(bytes));
            CsvReaderUtils csvReader = new CsvReaderUtils(reader);
            List<Map<String, String>> arr = csvReader.readAll();

            Tuple2<String, IocJsonLog> urlTuple = new Tuple2<>();
            Tuple2<String, IocJsonLog> domainTuple = new Tuple2<>();
            Tuple2<String, IocJsonLog> ipTuple = new Tuple2<>();

            for (Map<String, String> obj : arr) {
                //字典对象
                String iocType = obj.get("ioc_type");
                String iocValue = obj.get("ioc_value");
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

    private void setCommonField(Map<String, String> obj, IocJsonLog iocJsonLog) {
        iocJsonLog.setIoc_type(obj.get("ioc_type"));
        iocJsonLog.setIoc_value(obj.get("ioc_value"));
        iocJsonLog.setThreat_type(obj.get("threat_type"));
        iocJsonLog.setMalware_name(obj.get("malware_name"));
        //malware_alias
        if (StringUtil.isNotEmpty(obj.get("malware_alias"))) {
            iocJsonLog.setMalware_alias(obj.get("malware_alias"));
        } else {
            iocJsonLog.setMalware_alias(" ");
        }
        //mitre_attack_description
        if (StringUtil.isNotEmpty(obj.get("mitre_attack_description"))) {
            iocJsonLog.setMitre_attack_description(obj.get("mitre_attack_description"));
        } else {
            iocJsonLog.setMitre_attack_description(" ");
        }
        //mitre_attack_platforms
        if (StringUtil.isNotEmpty(obj.get("mitre_attack_platforms"))) {
            iocJsonLog.setMitre_attack_platforms(obj.get("mitre_attack_platforms"));
        } else {
            iocJsonLog.setMitre_attack_platforms(" ");
        }
        //mitre_attack_techniques
        if (StringUtil.isNotEmpty(obj.get("mitre_attack_techniques"))) {
            String mitreAttackTechniques = obj.get("mitre_attack_techniques").replace("\"", "");
            iocJsonLog.setMitre_attack_techniques(mitreAttackTechniques);
        } else {
            iocJsonLog.setMitre_attack_techniques(" ");
        }
        //mitre_attack_groups
        if (StringUtil.isNotEmpty(obj.get("mitre_attack_groups"))) {
            iocJsonLog.setMitre_attack_groups(obj.get("mitre_attack_groups").replace("\"", ""));
        } else {
            iocJsonLog.setMitre_attack_groups(" ");
        }
    }

    @Override
    public void cancel() {

    }
}
