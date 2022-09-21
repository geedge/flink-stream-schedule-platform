package com.zdjizhi.dns.pre;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.platform.Schedule;
import com.zdjizhi.base.utils.KafkaUtils;
import com.zdjizhi.dns.pre.common.CommonConfig;
import com.zdjizhi.dns.pre.common.DnsMetricLog;
import com.zdjizhi.dns.pre.function.*;
import com.zdjizhi.dns.pre.process.FirstAggregationProcess;
import com.zdjizhi.dns.pre.process.SecondAggregationReduce;
import com.zdjizhi.etl.CnRecordEtl;
import com.zdjizhi.utils.StringUtil;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class DnsPreMetric implements Schedule {


    @Override
    public void schedule() throws Exception {
        SingleOutputStreamOperator<CnRecordLog> source = CnRecordEtl.singleOutputStreamOperator;
        SingleOutputStreamOperator<DnsMetricLog> firstProcess = source.filter((log)->"DNS".equals(log.getCommon_schema_type()))
                .keyBy(CnRecordLog::getCommon_server_ip)
                .window(TumblingEventTimeWindows.of(Time.minutes(CommonConfig.DNS_PRE_METRICS_WINDOW_TIME)))
                .process(new FirstAggregationProcess());

        //metric_dns_server_ip
        SingleOutputStreamOperator<String> serverIpMetric = firstProcess.filter((log) -> StringUtil.isNotEmpty(log.getCommon_server_ip()))
                .keyBy(DnsMetricLog::getCommon_server_ip)
                .window(TumblingEventTimeWindows.of(Time.minutes(CommonConfig.DNS_PRE_METRICS_WINDOW_TIME)))
                .reduce(new SecondAggregationReduce(), new MetricDnsServerIpFunc());
        serverIpMetric.addSink(KafkaUtils.getKafkaSink(CommonConfig.METRIC_DNS_SERVER_IP_TOPIC)).setParallelism(CommonConfig.DNS_METRIC_OUTPUT_PARALLELISM);
//        serverIpMetric.print();

        //metric_dns_qname
        SingleOutputStreamOperator<String> qnameMetric = firstProcess.filter((log) -> StringUtil.isNotEmpty(log.getDns_qname()))
                .keyBy(DnsMetricLog::getDns_qname)
                .window(TumblingEventTimeWindows.of(Time.minutes(CommonConfig.DNS_PRE_METRICS_WINDOW_TIME)))
                .reduce(new SecondAggregationReduce(), new MetricDnsQnameFunc());
        qnameMetric.addSink(KafkaUtils.getKafkaSink(CommonConfig.METRIC_DNS_QNAME_TOPIC)).setParallelism(CommonConfig.DNS_METRIC_OUTPUT_PARALLELISM);

        //metric_dns_qtype
        SingleOutputStreamOperator<String> qtypeMetric = firstProcess.filter((log) -> StringUtil.isNotEmpty(log.getDns_qtype()))
                .keyBy(DnsMetricLog::getDns_qtype)
                .window(TumblingEventTimeWindows.of(Time.minutes(CommonConfig.DNS_PRE_METRICS_WINDOW_TIME)))
                .reduce(new SecondAggregationReduce(), new MetricDnsQtypeFunc());
        qtypeMetric.addSink(KafkaUtils.getKafkaSink(CommonConfig.METRIC_DNS_QTYPE_TOPIC)).setParallelism(CommonConfig.DNS_METRIC_OUTPUT_PARALLELISM);

        //metric_dns_rcode
        SingleOutputStreamOperator<String> rcodeMetric = firstProcess.filter((log) -> StringUtil.isNotEmpty(log.getDns_rcode()))
                .keyBy(DnsMetricLog::getDns_rcode)
                .window(TumblingEventTimeWindows.of(Time.minutes(CommonConfig.DNS_PRE_METRICS_WINDOW_TIME)))
                .reduce(new SecondAggregationReduce(), new MetricDnsRcodeFunc());
        rcodeMetric.addSink(KafkaUtils.getKafkaSink(CommonConfig.METRIC_DNS_RCODE_TOPIC)).setParallelism(CommonConfig.DNS_METRIC_OUTPUT_PARALLELISM);

        //metric_dns_rr_a
        SingleOutputStreamOperator<String> rraMetric = firstProcess.filter((log) -> StringUtil.isNotEmpty(log.getDns_rra()))
                .keyBy(DnsMetricLog::getDns_rra)
                .window(TumblingEventTimeWindows.of(Time.minutes(CommonConfig.DNS_PRE_METRICS_WINDOW_TIME)))
                .reduce(new SecondAggregationReduce(), new MetricDnsRraFunc());
        rraMetric.addSink(KafkaUtils.getKafkaSink(CommonConfig.METRIC_DNS_RRA_TOPIC)).setParallelism(CommonConfig.DNS_METRIC_OUTPUT_PARALLELISM);

        //metric_dns_rr_aaaa
        SingleOutputStreamOperator<String> rr4aMetric = firstProcess.filter((log) -> StringUtil.isNotEmpty(log.getDns_rraaaa()))
                .keyBy(DnsMetricLog::getDns_rraaaa)
                .window(TumblingEventTimeWindows.of(Time.minutes(CommonConfig.DNS_PRE_METRICS_WINDOW_TIME)))
                .reduce(new SecondAggregationReduce(), new MetricDnsRraaaaFunc());
        rr4aMetric.addSink(KafkaUtils.getKafkaSink(CommonConfig.METRIC_DNS_RRAAAA_TOPIC)).setParallelism(CommonConfig.DNS_METRIC_OUTPUT_PARALLELISM);

        //metric_dns_rr_cname
        SingleOutputStreamOperator<String> rrcnameMetric = firstProcess.filter((log) -> StringUtil.isNotEmpty(log.getDns_rrcname()))
                .keyBy(DnsMetricLog::getDns_rrcname)
                .window(TumblingEventTimeWindows.of(Time.minutes(CommonConfig.DNS_PRE_METRICS_WINDOW_TIME)))
                .reduce(new SecondAggregationReduce(), new MetricDnsRrcnameFunc());
        rrcnameMetric.addSink(KafkaUtils.getKafkaSink(CommonConfig.METRIC_DNS_RRCNAME_TOPIC)).setParallelism(CommonConfig.DNS_METRIC_OUTPUT_PARALLELISM);

    }


}
