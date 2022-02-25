package com.zdjizhi.security.operator;

import com.fasterxml.jackson.databind.JavaType;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.utils.SnowflakeId;
import com.zdjizhi.security.common.SecurityEventLog;
import com.zdjizhi.utils.DateUtils;
import com.zdjizhi.utils.JsonMapper;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;


/**
 * @ Author pengfeixu
 * @ Date 2022/2/24
 * @ Description
 */
public class SecurityWindowFunction extends ProcessWindowFunction<CnRecordLog, SecurityEventLog,String, TimeWindow> {
    private static Logger LOG = LoggerFactory.getLogger(SecurityWindowFunction.class);
    private static JsonMapper jsonMapperInstance = JsonMapper.getInstance();
    private static JavaType listType = jsonMapperInstance.createCollectionType(ArrayList.class, HashMap.class);
    //上一窗口的状态
    private MapState<String, SecurityEventLog> securityEventLogMapState;

    private String entityType;
    private static String iocJson = readJsonFile(com.zdjizhi.base.common.CommonConfig.IOC_PATH);

    public SecurityWindowFunction(String entityType) {
        this.entityType = entityType;
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        securityEventLogMapState=getRuntimeContext()
                .getMapState(new MapStateDescriptor<String, SecurityEventLog>("result"
                        ,Types.STRING
                        ,Types.POJO(SecurityEventLog.class)));

    }


    @Override
    public void process(String s, Context context, Iterable<CnRecordLog> elements, Collector<SecurityEventLog> out) throws Exception {
        Iterator<CnRecordLog> iterator = elements.iterator();
//        ValueState<String> currentKey = context.windowState().getState(new ValueStateDescriptor<String>("key", Types.STRING));

        while (iterator.hasNext()){
            CnRecordLog value = iterator.next();
            if (securityEventLogMapState.contains(s)){
                long recv_time = value.getCommon_recv_time();
                Date granularityFiveMinute = DateUtils.getGranularityFiveMinute(new Date(recv_time));
                long start_time = (granularityFiveMinute.getTime())/1000;
                long end_time =start_time+300;
                securityEventLog.setStart_time(recv_time>start_time ? start_time : recv_time);
                securityEventLog.setEnd_time(recv_time > end_time? recv_time : end_time );
                securityEventLog.setDuration_ms((end_time - start_time)*1000);
                securityEventLogMapState.put(s,securityEventLog);
                out.collect(securityEventLog);
            }else {
                initFirstLog(value);
                securityEventLogMapState.put(s,securityEventLog);
                out.collect(securityEventLog);
            }
        }
    }


    public void initFirstLog(CnRecordLog value){
        switch (entityType){
            case "ip:port":
                initEventIpLog(value);
                break;
            case "domain":

                initEventDomainLog(value);
                break;
            case "url":
                initEventUrlLog(value);
                break;
            default:
                throw new IllegalArgumentException();
        }

    }

    private static SecurityEventLog securityEventLog = new SecurityEventLog();

//    public void initEventIpLog(SecurityEventLog securityEventLog,CnRecordLog cnRecordLog){
    public void initEventIpLog(CnRecordLog cnRecordLog){
        ArrayList<HashMap<Object, Object>> jarray = jsonMapperInstance.fromJson(iocJson, listType);
        HashMap<Object, HashMap<Object, Object>> iocmap = new HashMap<>();
        for (HashMap<Object, Object> obj : jarray) {
            ArrayList<String> arry = new ArrayList<>();
//            cnRecordLog.setCommon_server_ip("79.134.225.79");
            if ("ip:port".equals(obj.get("ioc_type")) && obj.get("ioc_value").toString().contains(cnRecordLog.getCommon_server_ip())) {
//                securityEventLog.setEvent_id(Long.parseLong(obj.get("ioc_id").toString()));
                getMessage(cnRecordLog);
                securityEventLog.setIoc_type(obj.get("ioc_type").toString());
                if (obj.get("threat_type").equals("botnet_cc")) {
                    securityEventLog.setSecurity_type("common and control");
                } else if (obj.get("threat_type").equals("payload_delivery")) {
                    securityEventLog.setSecurity_type("payload delivery");
                }
                securityEventLog.setEvent_key(cnRecordLog.getCommon_server_ip() + " " + securityEventLog.getSecurity_type());
                //malware_name
                securityEventLog.setMalware_name(obj.get("malware_printable").toString());
                arry.add(obj.get("malware_alias").toString());
                securityEventLog.setMalware_alias(arry);
                securityEventLog.setEvent_severity("critical");
                HashMap<Object, Object> map1 = new HashMap<Object, Object>();
                map1.put("ip:port", securityEventLog);
                iocmap.put("ioc_type", map1);
//                getMessage(cnRecordLog);
//                return
//                System.out.println(securityEventLog);
            }
        }
    }

    public void initEventDomainLog(CnRecordLog cnRecordLog){
        ArrayList<HashMap<Object, Object>> jarray = jsonMapperInstance.fromJson(iocJson, listType);
        HashMap<Object, HashMap<Object, Object>> iocmap = new HashMap<>();
        for (HashMap<Object, Object> obj : jarray) {
            ArrayList<String> arry = new ArrayList<>();
//            cnRecordLog.setDomain("cosmokosmo.best");
                if ("domain".equals(obj.get("ioc_type")) && (obj.get("ioc_value").toString().equals(cnRecordLog.getDomain()) || obj.get("ioc_value").toString().contains(cnRecordLog.getDomain()))) {
                    getMessage(cnRecordLog);
                    securityEventLog.setIoc_type(obj.get("ioc_type").toString());
                    if (obj.get("threat_type").equals("botnet_cc")) {
                        securityEventLog.setSecurity_type("common and control");
                    } else if (obj.get("threat_type").equals("payload_delivery")) {
                        securityEventLog.setSecurity_type("payload delivery");
                    }
                    securityEventLog.setEvent_key(cnRecordLog.getDomain() + " " + securityEventLog.getSecurity_type());
                    //malware_name
                    securityEventLog.setMalware_name(obj.get("malware_printable").toString());
                    arry.add(obj.get("malware_alias").toString());
                    securityEventLog.setMalware_alias(arry);
                    securityEventLog.setEvent_severity("critical");
                    HashMap<Object, Object> map1 = new HashMap<Object, Object>();
                    map1.put("domain", securityEventLog);
                    iocmap.put("ioc_type", map1);
//                System.out.println(securityEventLog);
                }
            }
    }


    public void initEventUrlLog(CnRecordLog cnRecordLog){
        ArrayList<HashMap<Object, Object>> jarray = jsonMapperInstance.fromJson(iocJson, listType);
        HashMap<Object, HashMap<Object, Object>> iocmap = new HashMap<>();
        for (HashMap<Object, Object> obj : jarray) {
            ArrayList<String> arry = new ArrayList<>();
//            cnRecordLog.setHttp_url("http://appwebstat.biz/info.php");
                if ("domain".equals(obj.get("ioc_type")) && (obj.get("ioc_value").toString().equals(cnRecordLog.getHttp_url()) || obj.get("ioc_value").toString().contains(cnRecordLog.getHttp_url()))) {
                    getMessage(cnRecordLog);
                    securityEventLog.setIoc_type(obj.get("ioc_type").toString());
                    if (obj.get("threat_type").equals("botnet_cc")) {
                        securityEventLog.setSecurity_type("common and control");
                    } else if (obj.get("threat_type").equals("payload_delivery")) {
                        securityEventLog.setSecurity_type("payload delivery");
                    }
                    securityEventLog.setEvent_key(cnRecordLog.getHttp_url() + " " + securityEventLog.getSecurity_type());
                    //malware_name
                    securityEventLog.setMalware_name(obj.get("malware_printable").toString());
                    arry.add(obj.get("malware_alias").toString());
                    securityEventLog.setMalware_alias(arry);
                    securityEventLog.setEvent_severity("critical");
                    HashMap<Object, Object> map1 = new HashMap<Object, Object>();
                    map1.put("url", securityEventLog);
                    iocmap.put("ioc_type", map1);

//                System.out.println(securityEventLog);
                }
            }
    }


//    private void updateEventLog(SecurityEventLog eventLog, CnRecordLog value) {
//        eventLog.setStart_time(value.getCommon_recv_time() > eventLog.getStart_time() ? eventLog.getStart_time() : value.getCommon_recv_time());
//        eventLog.setEnd_time(value.getCommon_recv_time() > eventLog.getEnd_time() ? value.getCommon_recv_time() : eventLog.getEnd_time());
//        eventLog.setDuration_ms((eventLog.getEnd_time() - eventLog.getStart_time()) * 1000);
//    }

    private static String readJsonFile(String fileName) {
        String jsonStr;
        try {
            File jsonFile = new File(fileName);
            FileReader fileReader = new FileReader(jsonFile);
            Reader reader = new InputStreamReader(new FileInputStream(jsonFile), StandardCharsets.UTF_8);
            int ch;
            StringBuilder sb = new StringBuilder();
            while ((ch = reader.read()) != -1) {
                sb.append((char) ch);
            }
            fileReader.close();
            reader.close();
            jsonStr = sb.toString();
            return jsonStr;
        } catch (Exception e) {
            LOG.error(e.toString());
            return null;
        }
    }


    public static SecurityEventLog getMessage(CnRecordLog cnRecordLog){
        securityEventLog.setEvent_id(SnowflakeId.generateId());
        long recv_time = cnRecordLog.getCommon_recv_time();
        Date granularityFiveMinute = DateUtils.getGranularityFiveMinute(new Date(recv_time));
        long start_time = (granularityFiveMinute.getTime())/1000 ;
        long end_time=start_time+300;
//        securityEventLog.setStart_time(cnRecordLog.getCommon_recv_time());
        securityEventLog.setStart_time(start_time);
        securityEventLog.setEnd_time(end_time);
//        System.out.println(securityEventLog.getEnd_time());
        securityEventLog.setDuration_ms((end_time-start_time)*1000);
        securityEventLog.setVictim_ip(cnRecordLog.getCommon_client_ip());
        securityEventLog.setVictim_location_country(cnRecordLog.getClient_country());
        securityEventLog.setVictim_location_province(cnRecordLog.getClient_province());
        securityEventLog.setVictim_location_region(cnRecordLog.getClient_region());
        securityEventLog.setServer_ip(cnRecordLog.getCommon_server_ip());
        securityEventLog.setClient_ip(cnRecordLog.getCommon_client_ip());
        //offender_ip
        securityEventLog.setOffender_ip(cnRecordLog.getCommon_server_ip());
        securityEventLog.setOffender_location_country(cnRecordLog.getServer_country());
        securityEventLog.setOffender_location_province(cnRecordLog.getServer_province());
        securityEventLog.setOffender_location_region(cnRecordLog.getServer_region());
        //domain
        securityEventLog.setDomain(cnRecordLog.getDomain());
        securityEventLog.setDomain_category_name(cnRecordLog.getDomain_category_name());
        securityEventLog.setDomain_category_group(cnRecordLog.getDomain_category_group());
        securityEventLog.setDomain_reputation_score(cnRecordLog.getDomain_reputation_score());
        securityEventLog.setDomain_reputation_level(cnRecordLog.getDomain_reputation_level());
        //app
        securityEventLog.setApp_name(cnRecordLog.getCommon_app_label());
        securityEventLog.setApp_category(cnRecordLog.getApp_category());
        securityEventLog.setApp_subcategory(cnRecordLog.getApp_subcategory());
        securityEventLog.setApp_risk(cnRecordLog.getApp_risk());

        return securityEventLog;
    }


}
