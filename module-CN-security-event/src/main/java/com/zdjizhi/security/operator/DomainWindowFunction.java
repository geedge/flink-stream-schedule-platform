package com.zdjizhi.security.operator;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonInternalConfig;
import com.zdjizhi.base.utils.FileUtils;
import com.zdjizhi.etl.utils.CsvReaderUtils;
import com.zdjizhi.security.common.IocJsonLog;
import com.zdjizhi.security.common.SecurityEventLog;
import com.zdjizhi.security.handler.SecurityHandler;
import com.zdjizhi.security.utils.IocUtils;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ Author pengfeixu
 * @ Date 2022/3/4
 * @ Description
 */
public class DomainWindowFunction extends KeyedBroadcastProcessFunction<String, CnRecordLog, byte[], SecurityEventLog> {

    private static Logger LOG = LoggerFactory.getLogger(ServerIpWindowFunction.class);

    //上一窗口的状态
    private MapState<String, SecurityEventLog> previousMapState;

    private Map<String, IocJsonLog> iocMap = new HashMap<>();
    private ValueState<Long> currentTimer;
    private ValueState<SecurityEventLog> currentResult;

    @Override
    public void open(Configuration parameters) throws Exception {
        previousMapState = getRuntimeContext()
                .getMapState(new MapStateDescriptor<String, SecurityEventLog>("DomainResult"
                        , Types.STRING
                        , Types.POJO(SecurityEventLog.class)));
        currentTimer = getRuntimeContext().getState(new ValueStateDescriptor<>("domainTimer", Types.LONG));
        currentResult = getRuntimeContext().getState(new ValueStateDescriptor<>("domainResult", Types.POJO(SecurityEventLog.class)));
        System.out.println("begin init");
        String fileSuffix = ".csv";
        byte[] bytes = FileUtils.getFileBytes(CommonInternalConfig.IOC_TYPE + fileSuffix);
        updateIocData(bytes);
        System.out.println("init over");
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<SecurityEventLog> out) throws Exception {
        SecurityEventLog result = currentResult.value();
        if (result != null) {
            out.collect(result);
            previousMapState.put(ctx.getCurrentKey(), result);
            currentResult.clear();
        } else {
            previousMapState.clear();
        }
        currentTimer.clear();
    }

    @Override
    public void processElement(CnRecordLog value, ReadOnlyContext ctx, Collector<SecurityEventLog> out) throws Exception {
        try {
            Long curTimeStamp = currentTimer.value();
            if (curTimeStamp == null || curTimeStamp == 0) {
                long onTimer = ctx.timestamp() + com.zdjizhi.security.common.CommonConfig.FLINK_WINDOW_TIME * 60 * 1000;
                ctx.timerService().registerProcessingTimeTimer(onTimer);
                currentTimer.update(onTimer);
            }

            if (currentResult.value() == null) {
                String key = ctx.getCurrentKey();
                if (iocMap.containsKey(key)) {
                    if (previousMapState.contains(key)) {
                        SecurityEventLog securityEventLog = previousMapState.get(key);
                        SecurityEventLog eventLog = SecurityHandler.updateEventLog(securityEventLog, value);
                        currentResult.update(eventLog);
                    } else {
                        SecurityEventLog securityEventLog = new SecurityEventLog();
                        SecurityEventLog eventLog = initEventLog(securityEventLog, value);
                        currentResult.update(eventLog);
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("处理Domain:{} security失败\n", ctx.getCurrentKey(), e);
        }
    }

    @Override
    public void processBroadcastElement(byte[] value, Context ctx, Collector<SecurityEventLog> out) throws Exception {
        updateIocData(value);
    }

    private void updateIocData(byte[] iocBytes) {
        Map<String, IocJsonLog> newIocMap = new HashMap<>();

        InputStreamReader reader = new InputStreamReader(new ByteArrayInputStream(iocBytes));
        CsvReaderUtils csvReader = new CsvReaderUtils(reader);
        List<Map<String, String>> arr = csvReader.readAll();

        for (Map<String, String> obj : arr) {
            String iocType = obj.get("ioc_type");
            if ("domain".equals(iocType)) {
                String iocValue = obj.get("ioc_value");
                IocJsonLog iocJsonLog = new IocJsonLog();
                IocUtils.setCommonField(obj, iocJsonLog);
                newIocMap.put(iocValue, iocJsonLog);
            }
        }
        iocMap = newIocMap;
    }

    private SecurityEventLog initEventLog(SecurityEventLog securityEventLog, CnRecordLog cnRecordLog) {
        try {
            String key = cnRecordLog.getDomain();
            if (iocMap.containsKey(key)) {
                IocJsonLog iocJsonLog = iocMap.get(key);
                if ("domain".equals(iocJsonLog.getIoc_type()) && iocJsonLog.getIoc_value().equals(key)) {
                    SecurityHandler.setIocField(securityEventLog, key, iocJsonLog);
                    SecurityHandler.initMessage(securityEventLog, cnRecordLog);
                }
            }
            return securityEventLog;
        } catch (Exception e) {
            LOG.error(e.toString());
            return null;
        }

    }

}

