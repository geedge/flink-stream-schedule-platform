package com.zdjizhi.security;

import com.fasterxml.jackson.databind.JavaType;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.platform.Schedule;
import com.zdjizhi.base.utils.ESSinkUtils;
import com.zdjizhi.base.utils.FlinkEnvironmentUtils;
import com.zdjizhi.etl.CnRecordEtl;
import com.zdjizhi.security.common.CommonConfig;
import com.zdjizhi.security.operator.EsSinkFunctionImpl;
import com.zdjizhi.security.operator.SecurityWindowFunction;
import com.zdjizhi.utils.JsonMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.http.HttpHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
/**
 * @ Author pengfeixu
 * @ Description
 */
public class CnSecurityEvent implements Schedule {
    private static Logger LOG = LoggerFactory.getLogger(CnSecurityEvent.class);
    private static JsonMapper jsonMapperInstance = JsonMapper.getInstance();
    private static JavaType listType = jsonMapperInstance.createCollectionType(ArrayList.class, HashMap.class);

    @Override
    public void schedule() throws Exception {
        SingleOutputStreamOperator<CnRecordLog> cnRecordSource = CnRecordEtl.getCnRecordSource()
                .assignTimestampsAndWatermarks(
                        FlinkEnvironmentUtils.createWatermarkStrategy(
                                Duration.ofSeconds(CommonConfig.FLINK_WATERMARK_MAX_ORDERNESS),
                                (event, timestamp) -> event.getCommon_recv_time() * 1000
                        )
                );


        SingleOutputStreamOperator dictionaryPocess = cnRecordSource
                .filter((log) -> log.getCommon_server_ip() != null)
                .keyBy(CnRecordLog::getCommon_server_ip)
                .window(TumblingProcessingTimeWindows.of(Time.minutes(CommonConfig.FLINK_WINDOW_TIME)))
                .process(new SecurityWindowFunction());

        List<HttpHost> esAddresses = ESSinkUtils.getEsAddresses(com.zdjizhi.base.common.CommonConfig.ES_HOST);
        ESSinkUtils.addSink(esAddresses,CommonConfig.ES_MAX_FlUSH, dictionaryPocess, new EsSinkFunctionImpl());

        FlinkEnvironmentUtils.streamExeEnv.execute();
    }



    public static void main(String[] args) throws Exception {
        new CnSecurityEvent().schedule();
    }
}
