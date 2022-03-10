package com.zdjizhi.security.operator;

import com.fasterxml.jackson.databind.JavaType;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonConfig;

import com.zdjizhi.etl.utils.AppUtils;
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
public class ServerIpWindowFunction extends ProcessWindowFunction<CnRecordLog, SecurityEventLog,String, TimeWindow> {
    private static Logger LOG = LoggerFactory.getLogger(ServerIpWindowFunction.class);
    private static JsonMapper jsonMapperInstance = JsonMapper.getInstance();
    private static JavaType listType = jsonMapperInstance.createCollectionType(ArrayList.class, HashMap.class);
    //    private static SecurityEventLog securityEventLog=new SecurityEventLog() ;
    private static HashMap<String,IocJsonLog>iocmap=iocMessage();
    //上一窗口的状态
    private MapState<String, SecurityEventLog> securityEventLogMapState;


    @Override
    public void open(Configuration parameters) throws Exception {
        securityEventLogMapState=getRuntimeContext()
                .getMapState(new MapStateDescriptor<String, SecurityEventLog>("serverIpResult"
                        ,Types.STRING
                        ,Types.POJO(SecurityEventLog.class)));

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
                    SecurityEventLog eventLog = initEventLog(securityEventLog,value);
                    securityEventLogMapState.put(key, eventLog);
                    out.collect(eventLog);
                }
            }
        }catch (Exception e){
            LOG.error(e.toString());
        }
    }



    private  SecurityEventLog initEventLog(SecurityEventLog securityEventLog,CnRecordLog cnRecordLog) {
        try {
            if (iocmap.containsKey(cnRecordLog.getCommon_server_ip())){
                if ("ip:port".equals(iocmap.get(cnRecordLog.getCommon_server_ip()).getIoc_type())){
                    if ("botnet_cc".equals(iocmap.get(cnRecordLog.getCommon_server_ip()).getThreat_type())) {
                        securityEventLog.setSecurity_type("common and control");
                    } else if ("payload_delivery".equals(iocmap.get(cnRecordLog.getCommon_server_ip()).getThreat_type())) {
                        securityEventLog.setSecurity_type("payload delivery");
                    }
                    securityEventLog.setEvent_key(cnRecordLog.getCommon_server_ip()+ " " + securityEventLog.getSecurity_type());
                    securityEventLog.setMalware_name(iocmap.get(cnRecordLog.getCommon_server_ip()).getMalware_printable());
                    securityEventLog.setMalware_alias(iocmap.get(cnRecordLog.getCommon_server_ip()).getMalware_alias());
                    securityEventLog.setIoc_type("ip");
                    securityEventLog.setIoc_value(iocmap.get(cnRecordLog.getCommon_server_ip()).getIoc_value());
                    SecurityHandler.initMessage(securityEventLog,cnRecordLog);
                }
            }
            return securityEventLog;
        }catch (Exception e){
            LOG.error(e.toString());
            return null;
        }

    }



    private SecurityEventLog updateEventLog(SecurityEventLog securityEventLog,CnRecordLog cnRecordLog){
        long recv_time = cnRecordLog.getCommon_recv_time();
        Date granularityFiveMinute = DateUtils.getGranularityFiveMinute(new Date(recv_time*1000));
        long start_time = (granularityFiveMinute.getTime())/1000;
        long end_time =start_time+ com.zdjizhi.security.common.CommonConfig.END_TIME;
        securityEventLog.setStart_time(recv_time>start_time ? start_time : recv_time);
        securityEventLog.setEnd_time(recv_time > end_time? recv_time : end_time );
        securityEventLog.setDuration_ms((end_time - start_time)*1000);
        return securityEventLog;
    }


    private static HashMap<String,IocJsonLog> iocMessage(){
        HashMap<String, IocJsonLog> map = new HashMap<>();
        try {
            String iocJson = AppUtils.readJsonFile(CommonConfig.IOC_PATH);
            ArrayList<HashMap<String, Object>> arr = jsonMapperInstance.fromJson(iocJson, listType);
            for (HashMap<String, Object> obj : arr) {
                //字典对象
                IocJsonLog iocJsonLog = new IocJsonLog();
                if ("ip:port".equals(obj.get("ioc_type").toString())) {
                    iocJsonLog.setIoc_type(obj.get("ioc_type").toString());
                    String[] ioc_values = obj.get("ioc_value").toString().split(":");
                    iocJsonLog.setIoc_value(ioc_values[0]);
//                    iocJsonLog.setIoc_value(obj.get("ioc_value").toString());
                    iocJsonLog.setThreat_type(obj.get("threat_type").toString());
                    iocJsonLog.setMalware_printable(obj.get("malware_printable").toString());
                    if (StringUtil.isNotEmpty(obj.get("malware_alias").toString())) {
                        iocJsonLog.setMalware_alias(obj.get("malware_alias").toString());
                    }
                    map.put(ioc_values[0],iocJsonLog);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return map;
    }

}
