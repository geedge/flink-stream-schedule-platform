package com.zdjizhi.security.operator;

import com.fasterxml.jackson.databind.JavaType;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonConfig;

import com.zdjizhi.etl.utils.JsonUtils;
import com.zdjizhi.security.common.IocJsonLog;
import com.zdjizhi.security.common.SecurityEventLog;
import com.zdjizhi.security.handler.SecurityHandler;
import com.zdjizhi.utils.DateUtils;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


/**
 * @ Author pengfeixu
 * @ Date 2022/2/24
 * @ Description
 */
public class ServerIpWindowFunction extends ProcessWindowFunction<CnRecordLog, SecurityEventLog, String, TimeWindow> {
    private static Logger LOG = LoggerFactory.getLogger(ServerIpWindowFunction.class);
    private static JsonMapper jsonMapperInstance = JsonMapper.getInstance();
    private static JavaType listType = jsonMapperInstance.createCollectionType(ArrayList.class, HashMap.class);
    //    private static SecurityEventLog securityEventLog=new SecurityEventLog() ;
    private static HashMap<String, IocJsonLog> iocmap = new HashMap<>();
    //上一窗口的状态
    private MapState<String, SecurityEventLog> securityEventLogMapState;

    static {
        iocMessage();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        securityEventLogMapState = getRuntimeContext()
                .getMapState(new MapStateDescriptor<String, SecurityEventLog>("serverIpResult"
                        , Types.STRING
                        , Types.POJO(SecurityEventLog.class)));
//        iocmap=iocMessage();
    }


    @Override
    public void process(String key, Context context, Iterable<CnRecordLog> elements, Collector<SecurityEventLog> out) throws Exception {
        Iterator<CnRecordLog> iterator = elements.iterator();
        CnRecordLog value = iterator.next();
        try {
            if (iocmap.containsKey(key)) {
                if (securityEventLogMapState.contains(key)) {
                    SecurityEventLog securityEventLog = securityEventLogMapState.get(value.getCommon_server_ip());
                    SecurityEventLog eventLog = updateEventLog(securityEventLog, value);
                    securityEventLogMapState.put(key, eventLog);
                    out.collect(eventLog);
                } else {
                    SecurityEventLog securityEventLog = new SecurityEventLog();
                    SecurityEventLog eventLog = initEventLog(securityEventLog, value);
                    securityEventLogMapState.put(key, eventLog);
                    out.collect(eventLog);
                }
            }
        } catch (Exception e) {
            LOG.error(e.toString());
        }
    }


    private SecurityEventLog initEventLog(SecurityEventLog securityEventLog, CnRecordLog cnRecordLog) {
        try {
            if (iocmap.containsKey(cnRecordLog.getCommon_server_ip())) {
                if ("ip".equals(iocmap.get(cnRecordLog.getCommon_server_ip()).getIoc_type())) {
                    securityEventLog.setSecurity_type(iocmap.get(cnRecordLog.getCommon_server_ip()).getThreat_type());
                    securityEventLog.setEvent_key(cnRecordLog.getCommon_server_ip() + " " + securityEventLog.getSecurity_type());
                    securityEventLog.setMalware_name(iocmap.get(cnRecordLog.getCommon_server_ip()).getMalware_name());
                    securityEventLog.setMalware_alias(iocmap.get(cnRecordLog.getCommon_server_ip()).getMalware_alias());
                    securityEventLog.setIoc_type(iocmap.get(cnRecordLog.getCommon_server_ip()).getIoc_type());
                    securityEventLog.setIoc_value(iocmap.get(cnRecordLog.getCommon_server_ip()).getIoc_value());
                    securityEventLog.setMalware_description(iocmap.get(cnRecordLog.getCommon_server_ip()).getMitre_attack_description());
                    securityEventLog.setMalware_platforms(iocmap.get(cnRecordLog.getCommon_server_ip()).getMitre_attack_platforms());
                    securityEventLog.setMalware_techniques(iocmap.get(cnRecordLog.getCommon_server_ip()).getMitre_attack_techniques());
                    securityEventLog.setMalware_groups(iocmap.get(cnRecordLog.getCommon_server_ip()).getMitre_attack_groups());
                    SecurityHandler.initMessage(securityEventLog, cnRecordLog);
                }
            }
            return securityEventLog;
        } catch (Exception e) {
            LOG.error(e.toString());
            return null;
        }

    }


    private SecurityEventLog updateEventLog(SecurityEventLog securityEventLog, CnRecordLog cnRecordLog) {
        long recv_time = cnRecordLog.getCommon_recv_time();
        Date granularityFiveMinute = DateUtils.getGranularityFiveMinute(new Date(recv_time * 1000));
        long start_time = (granularityFiveMinute.getTime()) / 1000;
        long end_time = start_time + com.zdjizhi.security.common.CommonConfig.END_TIME;
        securityEventLog.setStart_time(recv_time > start_time ? start_time : recv_time);
        securityEventLog.setEnd_time(recv_time > end_time ? recv_time : end_time);
        securityEventLog.setDuration_ms((end_time - start_time) * 1000);
        return securityEventLog;
    }


    private static void iocMessage() {
//        HashMap<String, IocJsonLog> map = new HashMap<>();
        try {
            String iocJson = JsonUtils.readJsonFile(CommonConfig.IOC_PATH);
            ArrayList<HashMap<String, Object>> arr = jsonMapperInstance.fromJson(iocJson, listType);
            for (HashMap<String, Object> obj : arr) {
                //字典对象
                IocJsonLog iocJsonLog = new IocJsonLog();
                if ("ip".equals(obj.get("ioc_type").toString())) {
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
                        String mitre_attack_techniques = obj.get("mitre_attack_techniques").toString().replace("\"", "");
//                    String replace = mitre_attack_techniques.replace(",", " ");
                        iocJsonLog.setMitre_attack_techniques(mitre_attack_techniques);
                    } else {
                        iocJsonLog.setMitre_attack_techniques(" ");
                    }
                    //mitre_attack_groups
                    if (StringUtil.isNotEmpty(obj.get("mitre_attack_groups").toString())) {
                        iocJsonLog.setMitre_attack_groups(obj.get("mitre_attack_groups").toString().replace("\"", ""));
                    } else {
                        iocJsonLog.setMitre_attack_groups(" ");
                    }
//                    map.put(ioc_values[0],iocJsonLog);
                    iocmap.put(iocJsonLog.getIoc_value(), iocJsonLog);
                }
            }
        } catch (Exception e) {
            LOG.error("加载IOC库失败 \n",e);
        }
    }


    public static void main(String[] args) throws Exception {

        iocMessage();
        Set<String> keySet = iocmap.keySet();
        for (String key : keySet) {
            System.out.println(iocmap.get(key).toString());
        }
        System.out.println(iocmap.size());

    }
}
