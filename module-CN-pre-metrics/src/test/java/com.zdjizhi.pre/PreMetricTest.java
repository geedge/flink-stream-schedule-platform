package com.zdjizhi.pre;

import cn.hutool.core.io.file.FileReader;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.config.Configs;
import com.zdjizhi.base.utils.FlinkEnvironmentUtils;
import com.zdjizhi.pre.common.CnMetricLog;
import com.zdjizhi.pre.common.CommonConfig;
import com.zdjizhi.pre.common.MetricKeyConfig;


import com.zdjizhi.pre.common.MetricResultLog;
import com.zdjizhi.pre.function.MetricIpProcessWindowFunc;
import com.zdjizhi.pre.operator.FirstAggregation;
import com.zdjizhi.pre.operator.SecondAggregationReduce;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;
import java.time.Duration;

import static org.junit.Assert.assertEquals;

public class PreMetricTest {

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build()
            );


    @Test
    public void jobTest() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.getConfig().setAutoWatermarkInterval(1L);

        DataStreamSource<CnRecordLog> source = env.addSource(new RichSourceFunction<CnRecordLog>() {
            @Override
            public void run(SourceContext<CnRecordLog> ctx) throws Exception {
                String path = Thread.currentThread().getContextClassLoader().getResource("cn-session-pre.json").getPath();
                FileReader fileReader = FileReader.create(new File(path));
                String s = fileReader.getReader().readLine();
                JSONArray objects = JSON.parseArray(s);
                for (Object object : objects) {
                    CnRecordLog cnRecordLog = JSON.parseObject(object.toString(), CnRecordLog.class);
                    Thread.sleep(100L);
                    ctx.collect(cnRecordLog);
                }
            }

            @Override
            public void cancel() {

            }
        });
        SingleOutputStreamOperator<CnMetricLog> firstProcess = source.assignTimestampsAndWatermarks(WatermarkStrategy
                .<CnRecordLog>forBoundedOutOfOrderness(Duration.ofSeconds(60))
                .withTimestampAssigner((event, timestamp) -> event.getCommon_recv_time() * 1000)).process(new FirstAggregation()).name("dnsProcess");
        SingleOutputStreamOperator<String> reduce = firstProcess.filter((log) -> log.getMetricKey() == MetricKeyConfig.SERVER_IP)
                .keyBy(CnMetricLog::getCommon_server_ip)
                .window(TumblingEventTimeWindows.of(Time.minutes(Configs.get(CommonConfig.PRE_METRICS_WINDOW_TIME))))
                .reduce(new SecondAggregationReduce(), new MetricIpProcessWindowFunc("server"));
        reduce.addSink(new RichSinkFunction<String>() {
            @Override
            public void invoke(String value, Context context) throws Exception {
                super.invoke(value, context);
                System.out.println(value);
                MetricResultLog log = JSON.parseObject(value, MetricResultLog.class);
                assertEquals("", log.getPkt_retrans_ratio().toString(), "0.0909");
            }
        });

        env.execute();
    }
}
