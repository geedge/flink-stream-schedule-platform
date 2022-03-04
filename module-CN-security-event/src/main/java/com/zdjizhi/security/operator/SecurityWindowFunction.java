package com.zdjizhi.security.operator;

import com.fasterxml.jackson.databind.JavaType;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.utils.SnowflakeId;
import com.zdjizhi.etl.utils.AppUtils;
import com.zdjizhi.security.common.IocJsonLog;
import com.zdjizhi.security.common.SecurityEventLog;
import com.zdjizhi.security.utils.DomainUtils;
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

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;


/**
 * @ Author pengfeixu
 * @ Date 2022/2/24
 * @ Description
 * //todo 新建一个字典类，将json文件赋值给字典类
 * //todo 通过字典对象和原始日志对象进行匹配
 * //todo 匹配成功则赋值给输出对象
 */
public class SecurityWindowFunction extends ProcessWindowFunction<CnRecordLog, SecurityEventLog,String, TimeWindow> {
    private static Logger LOG = LoggerFactory.getLogger(SecurityWindowFunction.class);
    private static JsonMapper jsonMapperInstance = JsonMapper.getInstance();
    private static JavaType listType = jsonMapperInstance.createCollectionType(ArrayList.class, HashMap.class);
    private static SecurityEventLog securityEventLog=new SecurityEventLog() ;
    private static HashMap<String,IocJsonLog>iocmap=iocMessage();
    //上一窗口的状态
    private MapState<String, SecurityEventLog> securityEventLogMapState;


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
        while (iterator.hasNext()){
            CnRecordLog value = iterator.next();
            if (securityEventLogMapState.contains(s)){
                long recv_time = value.getCommon_recv_time();
                Date granularityFiveMinute = DateUtils.getGranularityFiveMinute(new Date(recv_time*1000));
                long start_time = (granularityFiveMinute.getTime())/1000;
                long end_time =start_time+300;
                securityEventLog.setStart_time(recv_time>start_time ? start_time : recv_time);
                securityEventLog.setEnd_time(recv_time > end_time? recv_time : end_time );
                securityEventLog.setDuration_ms((end_time - start_time)*1000);
                securityEventLogMapState.put(s,securityEventLog);
                out.collect(securityEventLog);
            }else {
                initEventLog(value);
                securityEventLogMapState.put(s,securityEventLog);
                out.collect(securityEventLog);
            }
        }
    }


    private  void initEventLog(CnRecordLog cnRecordLog) {
        try {
//        cnRecordLog.setCommon_server_ip("79.134.225.79");
//        cnRecordLog.setDomain("cosmokosmo.best");
//        cnRecordLog.setHttp_url("http://appwebstat.biz/info.php");
            DomainUtils domainUtils = new DomainUtils();
            String domain = domainUtils.getDomain(cnRecordLog.getDomain(), 3);
            String url = domainUtils.getUrl(cnRecordLog.getHttp_url()+"/");
            if (iocmap.containsKey(cnRecordLog.getCommon_server_ip())){
                if ("ip:port".equals(iocmap.get(cnRecordLog.getCommon_server_ip()).getIoc_type())){
                    if ("botnet_cc".equals(iocmap.get(cnRecordLog.getCommon_server_ip()).getThreat_type())) {
                        securityEventLog.setSecurity_type("common and control");
                    } else if ("payload_delivery".equals(iocmap.get(cnRecordLog.getCommon_server_ip()).getThreat_type())) {
                        securityEventLog.setSecurity_type("payload delivery");
                    }
                    securityEventLog.setEvent_key(cnRecordLog.getCommon_server_ip()+ " " + securityEventLog.getSecurity_type());
                    securityEventLog.setEvent_id(SnowflakeId.generateId());
                    securityEventLog.setMalware_name(iocmap.get(cnRecordLog.getCommon_server_ip()).getMalware_printable());
                    securityEventLog.setMalware_alias(iocmap.get(cnRecordLog.getCommon_server_ip()).getMalware_aliass());
                    long recv_time = cnRecordLog.getCommon_recv_time();
                    Date granularityFiveMinute = DateUtils.getGranularityFiveMinute(new Date(recv_time * 1000));
                    long start_time = (granularityFiveMinute.getTime()) / 1000;
                    long end_time = start_time + 300;
                    securityEventLog.setStart_time(start_time);
                    securityEventLog.setEnd_time(end_time);
                    securityEventLog.setDuration_ms((end_time - start_time) * 1000);
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
                    securityEventLog.setEvent_severity("critical");
                }
            } else if (iocmap.containsKey(cnRecordLog.getDomain())){
                if ("domain".equals(iocmap.get(cnRecordLog.getDomain()).getIoc_type()) && (iocmap.get(cnRecordLog.getDomain()).getIoc_value().equals(cnRecordLog.getDomain()) || domain.contains(iocmap.get(cnRecordLog.getDomain()).getIoc_value()))){
                    if ("botnet_cc".equals(iocmap.get(cnRecordLog.getDomain()).getThreat_type())) {
                        securityEventLog.setSecurity_type("common and control");
                    } else if ("payload_delivery".equals(iocmap.get(cnRecordLog.getDomain()).getThreat_type())) {
                        securityEventLog.setSecurity_type("payload delivery");
                    }
                    securityEventLog.setEvent_key(cnRecordLog.getDomain()+ " " + securityEventLog.getSecurity_type());
                    securityEventLog.setEvent_id(SnowflakeId.generateId());
                    securityEventLog.setMalware_name(iocmap.get(cnRecordLog.getDomain()).getMalware_printable());
                    securityEventLog.setMalware_alias(iocmap.get(cnRecordLog.getDomain()).getMalware_aliass());
                    long recv_time = cnRecordLog.getCommon_recv_time();
                    Date granularityFiveMinute = DateUtils.getGranularityFiveMinute(new Date(recv_time * 1000));
                    long start_time = (granularityFiveMinute.getTime()) / 1000;
                    long end_time = start_time + 300;
                    securityEventLog.setStart_time(start_time);
                    securityEventLog.setEnd_time(end_time);
                    securityEventLog.setDuration_ms((end_time - start_time) * 1000);
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
                    securityEventLog.setEvent_severity("critical");
                }
            } else if (iocmap.containsKey(cnRecordLog.getHttp_url())){
                if ("url".equals(iocmap.get(cnRecordLog.getHttp_url()).getIoc_type()) && (iocmap.get(cnRecordLog.getHttp_url()).getIoc_value().equals(cnRecordLog.getHttp_url()) || url.contains(iocmap.get(cnRecordLog.getHttp_url()).getIoc_value()))){
                    if ("botnet_cc".equals(iocmap.get(cnRecordLog.getHttp_url()).getThreat_type())) {
                        securityEventLog.setSecurity_type("common and control");
                    } else if ("payload_delivery".equals(iocmap.get(cnRecordLog.getHttp_url()).getThreat_type())) {
                        securityEventLog.setSecurity_type("payload delivery");
                    }
                    securityEventLog.setEvent_key(cnRecordLog.getHttp_url()+ " " + securityEventLog.getSecurity_type());
                    securityEventLog.setEvent_id(SnowflakeId.generateId());
                    securityEventLog.setMalware_name(iocmap.get(cnRecordLog.getHttp_url()).getMalware_printable());
                    securityEventLog.setMalware_alias(iocmap.get(cnRecordLog.getHttp_url()).getMalware_aliass());
                    long recv_time = cnRecordLog.getCommon_recv_time();
                    Date granularityFiveMinute = DateUtils.getGranularityFiveMinute(new Date(recv_time * 1000));
                    long start_time = (granularityFiveMinute.getTime()) / 1000;
                    long end_time = start_time + 300;
                    securityEventLog.setStart_time(start_time);
                    securityEventLog.setEnd_time(end_time);
                    securityEventLog.setDuration_ms((end_time - start_time) * 1000);
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
                    securityEventLog.setEvent_severity("critical");
                }

            }
        }catch (Exception e){
            LOG.error(e.toString());
        }

    }


    private static HashMap<String,IocJsonLog> iocMessage(){
        HashMap<String, IocJsonLog> map = new HashMap<>();
        try {
            String iocJson = AppUtils.readJsonFile(CommonConfig.IOC_PATH);
            ArrayList<HashMap<String, Object>> arr = jsonMapperInstance.fromJson(iocJson, listType);
            for (HashMap<String, Object> obj : arr) {
                ArrayList<String> mal_alias = new ArrayList<>();
                //字典对象
                IocJsonLog iocJsonLog = new IocJsonLog();
                if ("ip:port".equals(obj.get("ioc_type").toString())) {
                    iocJsonLog.setIoc_type(obj.get("ioc_type").toString());
                    String[] ioc_values = obj.get("ioc_value").toString().split(":");
                    iocJsonLog.setIoc_value(ioc_values[0]);
                    iocJsonLog.setThreat_type(obj.get("threat_type").toString());
                    iocJsonLog.setMalware_printable(obj.get("malware_printable").toString());
                    if (StringUtil.isNotEmpty(obj.get("malware_alias").toString())) {
                        String[] split = obj.get("malware_alias").toString().split(",");
                        for (String s : split) {
                            mal_alias.add(s);
                        }
                    }
                    iocJsonLog.setMalware_aliass(mal_alias);
                    map.put(ioc_values[0],iocJsonLog);
                }else if ("domain".equals(obj.get("ioc_type").toString())) {
                    iocJsonLog.setIoc_type(obj.get("ioc_type").toString());
                    iocJsonLog.setIoc_value(obj.get("ioc_value").toString());
                    iocJsonLog.setThreat_type(obj.get("threat_type").toString());
                    iocJsonLog.setMalware_printable(obj.get("malware_printable").toString());
                    if (StringUtil.isNotEmpty(obj.get("malware_alias").toString())) {
                        String[] split = obj.get("malware_alias").toString().split(",");
                        for (String s : split) {
                            mal_alias.add(s);
                        }
                    }
                    iocJsonLog.setMalware_aliass(mal_alias);
                    map.put(obj.get("ioc_value").toString(),iocJsonLog);
                } else if (("url" ).equals(obj.get("ioc_type").toString())) {
                    iocJsonLog.setIoc_type(obj.get("ioc_type").toString());
                    iocJsonLog.setIoc_value(obj.get("ioc_value").toString());
                    iocJsonLog.setThreat_type(obj.get("threat_type").toString());
                    iocJsonLog.setMalware_printable(obj.get("malware_printable").toString());
                    if (StringUtil.isNotEmpty(obj.get("malware_alias").toString())) {
                        String[] split = obj.get("malware_alias").toString().split(",");
                        for (String s : split) {
                            mal_alias.add(s);
                        }
                    }
                    iocJsonLog.setMalware_aliass(mal_alias);
                    map.put(obj.get("ioc_value").toString(),iocJsonLog);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return map;
    }


}
