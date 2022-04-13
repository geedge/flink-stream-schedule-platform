package com.zdjizhi.top;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.platform.Schedule;
import com.zdjizhi.base.utils.FlinkEnvironmentUtils;
import com.zdjizhi.base.utils.KafkaUtils;
import com.zdjizhi.etl.CnRecordEtl;
import com.zdjizhi.top.common.TopBaseLog;
import com.zdjizhi.top.operator.TopMetricAggregate;
import com.zdjizhi.top.operator.TopMetricProcess;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Objects;

public class CnTopMetrics implements Schedule {
    @Override
    public void schedule() throws Exception {
        SingleOutputStreamOperator<CnRecordLog> source = CnRecordEtl.getCnRecordSource()
                .assignTimestampsAndWatermarks(
                        FlinkEnvironmentUtils.createWatermarkStrategy(
                                Duration.ofSeconds(com.zdjizhi.top.common.CommonConfig.TOP_METRICS_MAX_ORDERNESS),
                                (event, timestamp) -> event.getCommon_recv_time() * 1000
                        )
                );
        clientIpMetricsProcess(source);
        serverIpMetricsProcess(source);
        domainMetricsProcess(source);
        appLableMetricsProcess(source);
        countryCityMetrics(source);
    }

    private static void clientIpMetricsProcess(SingleOutputStreamOperator<CnRecordLog> source) {
        WindowedStream<TopBaseLog, Long, TimeWindow> clientIpView = source
                .filter((log) -> StringUtil.isNotEmpty(log.getCommon_client_ip()))
                .keyBy(CnRecordLog::getCommon_client_ip)
                .window(TumblingEventTimeWindows.of(Time.seconds(com.zdjizhi.top.common.CommonConfig.TOP_METRICS_WINDOW_TIME)))
                .aggregate(new TopMetricAggregate("common_client_ip", false), new MyWindowFunction())
                .keyBy(TopBaseLog::getCommon_recv_time)
                .window(TumblingEventTimeWindows.of(Time.seconds(com.zdjizhi.top.common.CommonConfig.TOP_METRICS_WINDOW_TIME)));

        commonMetrics(clientIpView, "common_client_ip");
    }

    private static void serverIpMetricsProcess(SingleOutputStreamOperator<CnRecordLog> source) {
        WindowedStream<TopBaseLog, Long, TimeWindow> serverIpView = source
                .filter((log) -> StringUtil.isNotEmpty(log.getCommon_server_ip()))
                .keyBy(CnRecordLog::getCommon_server_ip)
                .window(TumblingEventTimeWindows.of(Time.seconds(com.zdjizhi.top.common.CommonConfig.TOP_METRICS_WINDOW_TIME)))
                .aggregate(new TopMetricAggregate("common_server_ip", false), new MyWindowFunction())
                .keyBy(TopBaseLog::getCommon_recv_time)
                .window(TumblingEventTimeWindows.of(Time.seconds(com.zdjizhi.top.common.CommonConfig.TOP_METRICS_WINDOW_TIME)));

        commonMetrics(serverIpView, "common_server_ip");
    }

    private static void domainMetricsProcess(SingleOutputStreamOperator<CnRecordLog> source) {
        WindowedStream<TopBaseLog, Long, TimeWindow> domainView = source
                .filter((log) -> StringUtil.isNotEmpty(log.getDomain()))
                .keyBy(CnRecordLog::getDomain)
                .window(TumblingEventTimeWindows.of(Time.seconds(com.zdjizhi.top.common.CommonConfig.TOP_METRICS_WINDOW_TIME)))
                .aggregate(new TopMetricAggregate("domain", false), new MyWindowFunction())
                .keyBy(TopBaseLog::getCommon_recv_time)
                .window(TumblingEventTimeWindows.of(Time.seconds(com.zdjizhi.top.common.CommonConfig.TOP_METRICS_WINDOW_TIME)));

        commonMetrics(domainView, "domain");
    }

    private static void appLableMetricsProcess(SingleOutputStreamOperator<CnRecordLog> source) {
        WindowedStream<TopBaseLog, Long, TimeWindow> appLableView = source
                .filter((log) -> StringUtil.isNotEmpty(log.getCommon_app_label()))
                .keyBy(CnRecordLog::getCommon_app_label)
                .window(TumblingEventTimeWindows.of(Time.seconds(com.zdjizhi.top.common.CommonConfig.TOP_METRICS_WINDOW_TIME)))
                .aggregate(new TopMetricAggregate("common_app_label", false), new MyWindowFunction())
                .keyBy(TopBaseLog::getCommon_recv_time)
                .window(TumblingEventTimeWindows.of(Time.seconds(com.zdjizhi.top.common.CommonConfig.TOP_METRICS_WINDOW_TIME)));

        commonMetrics(appLableView, "common_app_label");
    }

    private static void countryCityMetrics(SingleOutputStreamOperator<CnRecordLog> source) {
        SingleOutputStreamOperator<String> aggregate = source
                .filter((log) -> StringUtil.isNotEmpty(log.getClient_country()) || StringUtil.isNotEmpty(log.getServer_country()))
                .keyBy(new KeysSelector())
                .window(TumblingEventTimeWindows.of(Time.seconds(com.zdjizhi.top.common.CommonConfig.TOP_METRICS_WINDOW_TIME)))
                .aggregate(new TopMetricAggregate("region", true))
                .filter(Objects::nonNull)
                .map(JsonMapper::toJsonString);

        aggregate.addSink(KafkaUtils.getKafkaSink(CommonConfig.TSG_BASE_COUNTRY_CITY_TOPIC))
                .setParallelism(CommonConfig.TSG_BASE_COUNTRY_CITY_PARALLELISM);
    }

    private static void commonMetrics(WindowedStream<TopBaseLog, Long, TimeWindow> view, String groupBy) {
        DataStream<String> union = topMetricsProcess(view, groupBy, "sessions")
                .union(topMetricsProcess(view, groupBy, "packets"))
                .union(topMetricsProcess(view, groupBy, "bytes"))
                .union(topMetricsProcess(view, groupBy, "establish_latency"))
                .union(topMetricsProcess(view, groupBy, "http_response_latency"))
                .union(topMetricsProcess(view, groupBy, "ssl_con_latency"))
                .union(topMetricsProcess(view, groupBy, "packet_loss"))
                .union(topMetricsProcess(view, groupBy, "packet_retrans"));

        union.addSink(KafkaUtils.getKafkaSink(CommonConfig.TSG_BASE_TOP_TOPIC))
                .setParallelism(CommonConfig.TSG_BASE_TOP_PARALLELISM);

    }

    private static SingleOutputStreamOperator<String> topMetricsProcess(WindowedStream<TopBaseLog, Long, TimeWindow> source,
                                                                        String groupBy,
                                                                        String orderBy) {
        SingleOutputStreamOperator<String> map = source.process(new TopMetricProcess(groupBy, orderBy)).filter(Objects::nonNull).map(JsonMapper::toJsonString);
        switch (com.zdjizhi.top.common.CommonConfig.TOP_DEFAULT_SLOTSHARINGGROUP_TYPE) {
            case 1:
                return map.slotSharingGroup(groupBy).setParallelism(com.zdjizhi.top.common.CommonConfig.TOP_METRIC_PARALLELISM);
            case 0:
                return map.slotSharingGroup("default").setParallelism(com.zdjizhi.top.common.CommonConfig.TOP_METRIC_PARALLELISM);
            default:
                throw new IllegalArgumentException();
        }
    }

    private static class KeysSelector implements KeySelector<CnRecordLog, Tuple8<String, String, String, String, String, String, String, String>> {
        @Override
        public Tuple8<String, String, String, String, String, String, String, String> getKey(CnRecordLog log) throws Exception {

            return Tuple8.of(log.getServer_country(), log.getClient_country(), log.getServer_province(),
                    log.getClient_province(), log.getServer_region(), log.getClient_region(), log.getCommon_schema_type(), log.getCommon_l4_protocol());
        }
    }

    private static class MyWindowFunction implements WindowFunction<TopBaseLog, TopBaseLog, String, TimeWindow> {
        @Override
        public void apply(String s, TimeWindow window, Iterable<TopBaseLog> input, Collector<TopBaseLog> out) throws Exception {
            TopBaseLog baseLog = input.iterator().next();
            baseLog.setCommon_recv_time(window.getEnd() / 1000);
            out.collect(baseLog);
        }
    }

    public static void main(String[] args) throws Exception {
        new CnTopMetrics().schedule();
    }


}
