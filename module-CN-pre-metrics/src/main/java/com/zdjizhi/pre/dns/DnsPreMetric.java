package com.zdjizhi.pre.dns;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.platform.Schedule;
import com.zdjizhi.base.utils.KafkaUtils;
import com.zdjizhi.etl.CnRecordEtl;
import com.zdjizhi.pre.common.MetricKeyConfig;
import com.zdjizhi.pre.dns.common.CommonConfig;
import com.zdjizhi.pre.dns.common.DnsMetricLog;
import com.zdjizhi.pre.dns.function.*;
import com.zdjizhi.pre.dns.process.FirstAggregation;
import com.zdjizhi.pre.dns.process.SecondAggregationReduce;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class DnsPreMetric implements Schedule {
    @Override
    public void schedule() throws Exception {
        SingleOutputStreamOperator<CnRecordLog> source = CnRecordEtl.singleOutputStreamOperator;
        SingleOutputStreamOperator<DnsMetricLog> firstProcess = source.filter((log) -> "DNS".equals(log.getCommon_schema_type()))
                .process(new FirstAggregation()).name("dnsProcess");

        //metric_dns_server_ip
        SingleOutputStreamOperator<String> serverIpMetric = firstProcess.filter((log) -> log.getMetricKey() == MetricKeyConfig.DNS_SERVER_IP)
                .keyBy(DnsMetricLog::getCommon_server_ip)
                .window(TumblingEventTimeWindows.of(Time.minutes(CommonConfig.DNS_PRE_METRICS_WINDOW_TIME)))
                .reduce(new SecondAggregationReduce(), new MetricDnsServerIpFunc());
        serverIpMetric.addSink(KafkaUtils.getKafkaSink(CommonConfig.METRIC_DNS_SERVER_IP_TOPIC)).setParallelism(CommonConfig.DNS_METRIC_OUTPUT_PARALLELISM);

        //metric_dns_qname
        SingleOutputStreamOperator<String> qnameMetric = firstProcess.filter((log) -> log.getMetricKey() == MetricKeyConfig.DNS_QNAME)
                .keyBy(DnsMetricLog::getDns_qname)
                .window(TumblingEventTimeWindows.of(Time.minutes(CommonConfig.DNS_PRE_METRICS_WINDOW_TIME)))
                .reduce(new SecondAggregationReduce(), new MetricDnsQnameFunc());
        qnameMetric.addSink(KafkaUtils.getKafkaSink(CommonConfig.METRIC_DNS_QNAME_TOPIC)).setParallelism(CommonConfig.DNS_METRIC_OUTPUT_PARALLELISM);

        //metric_dns_qtype
        SingleOutputStreamOperator<String> qtypeMetric = firstProcess.filter((log) -> log.getMetricKey() == MetricKeyConfig.DNS_QTYPE)
                .keyBy(DnsMetricLog::getDns_qtype)
                .window(TumblingEventTimeWindows.of(Time.minutes(CommonConfig.DNS_PRE_METRICS_WINDOW_TIME)))
                .reduce(new SecondAggregationReduce(), new MetricDnsQtypeFunc());
        qtypeMetric.addSink(KafkaUtils.getKafkaSink(CommonConfig.METRIC_DNS_QTYPE_TOPIC)).setParallelism(CommonConfig.DNS_METRIC_OUTPUT_PARALLELISM);

        //metric_dns_rcode
        SingleOutputStreamOperator<String> rcodeMetric = firstProcess.filter((log) -> log.getMetricKey() == MetricKeyConfig.DNS_RCODE)
                .keyBy(DnsMetricLog::getDns_rcode)
                .window(TumblingEventTimeWindows.of(Time.minutes(CommonConfig.DNS_PRE_METRICS_WINDOW_TIME)))
                .reduce(new SecondAggregationReduce(), new MetricDnsRcodeFunc());
        rcodeMetric.addSink(KafkaUtils.getKafkaSink(CommonConfig.METRIC_DNS_RCODE_TOPIC)).setParallelism(CommonConfig.DNS_METRIC_OUTPUT_PARALLELISM);

        //metric_dns_rr_a
        SingleOutputStreamOperator<String> rraMetric = firstProcess.filter((log) -> log.getMetricKey() == MetricKeyConfig.DNS_RRA)
                .keyBy(DnsMetricLog::getDns_rra)
                .window(TumblingEventTimeWindows.of(Time.minutes(CommonConfig.DNS_PRE_METRICS_WINDOW_TIME)))
                .reduce(new SecondAggregationReduce(), new MetricDnsRraFunc());
        rraMetric.addSink(KafkaUtils.getKafkaSink(CommonConfig.METRIC_DNS_RRA_TOPIC)).setParallelism(CommonConfig.DNS_METRIC_OUTPUT_PARALLELISM);

        //metric_dns_rr_aaaa
        SingleOutputStreamOperator<String> rr4aMetric = firstProcess.filter((log) -> log.getMetricKey() == MetricKeyConfig.DNS_RRAAAA)
                .keyBy(DnsMetricLog::getDns_rraaaa)
                .window(TumblingEventTimeWindows.of(Time.minutes(CommonConfig.DNS_PRE_METRICS_WINDOW_TIME)))
                .reduce(new SecondAggregationReduce(), new MetricDnsRraaaaFunc());
        rr4aMetric.addSink(KafkaUtils.getKafkaSink(CommonConfig.METRIC_DNS_RRAAAA_TOPIC)).setParallelism(CommonConfig.DNS_METRIC_OUTPUT_PARALLELISM);

        //metric_dns_rr_cname
        SingleOutputStreamOperator<String> rrcnameMetric = firstProcess.filter((log) -> log.getMetricKey() == MetricKeyConfig.DNS_RRCNAME)
                .keyBy(DnsMetricLog::getDns_rrcname)
                .window(TumblingEventTimeWindows.of(Time.minutes(CommonConfig.DNS_PRE_METRICS_WINDOW_TIME)))
                .reduce(new SecondAggregationReduce(), new MetricDnsRrcnameFunc());
        rrcnameMetric.addSink(KafkaUtils.getKafkaSink(CommonConfig.METRIC_DNS_RRCNAME_TOPIC)).setParallelism(CommonConfig.DNS_METRIC_OUTPUT_PARALLELISM);

    }


}
