package com.zdjizhi.security;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.platform.Schedule;
import com.zdjizhi.base.utils.ESSinkUtils;
import com.zdjizhi.base.utils.FlinkEnvironmentUtils;
import com.zdjizhi.etl.CnRecordEtl;
import com.zdjizhi.security.common.CommonConfig;
import com.zdjizhi.security.common.IocJsonLog;
import com.zdjizhi.security.common.SecurityEventLog;
import com.zdjizhi.security.operator.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.http.HttpHost;

import java.util.List;

/**
 * @ Author pengfeixu
 * @ Description
 */
public class CnSecurityEvent implements Schedule {

    @Override
    public void schedule() throws Exception {
        SingleOutputStreamOperator<CnRecordLog> cnRecordSource = CnRecordEtl.singleOutputStreamOperator;

        DataStreamSource<Tuple3<Tuple2<String, IocJsonLog>, Tuple2<String, IocJsonLog>, Tuple2<String, IocJsonLog>>> iocData = FlinkEnvironmentUtils.streamExeEnv.addSource(new IocSourceFunction());

        KeyedStream<Tuple2<String, IocJsonLog>, String> ipDataStream = iocData
                .map(new MapFunction<Tuple3<Tuple2<String, IocJsonLog>, Tuple2<String, IocJsonLog>, Tuple2<String, IocJsonLog>>, Tuple2<String, IocJsonLog>>() {
                    @Override
                    public Tuple2<String, IocJsonLog> map(Tuple3<Tuple2<String, IocJsonLog>, Tuple2<String, IocJsonLog>, Tuple2<String, IocJsonLog>> value) throws Exception {
                        return value.f0;
                    }
                })
                .filter((FilterFunction<Tuple2<String, IocJsonLog>>) value -> value.f0 != null)
                .keyBy((KeySelector<Tuple2<String, IocJsonLog>, String>) value -> value.f0);

        SingleOutputStreamOperator<SecurityEventLog> ipProcess = cnRecordSource
                .filter((log) -> log.getCommon_server_ip() != null)
                .keyBy(CnRecordLog::getCommon_server_ip)
                .connect(ipDataStream)
                .process(new ServerIpWindowFunction())
                .setParallelism(CommonConfig.SECURITY_SERVER_PARALLELISM);

        KeyedStream<Tuple2<String, IocJsonLog>, String> domainDataStream = iocData
                .map(new MapFunction<Tuple3<Tuple2<String, IocJsonLog>, Tuple2<String, IocJsonLog>, Tuple2<String, IocJsonLog>>, Tuple2<String, IocJsonLog>>() {
                    @Override
                    public Tuple2<String, IocJsonLog> map(Tuple3<Tuple2<String, IocJsonLog>, Tuple2<String, IocJsonLog>, Tuple2<String, IocJsonLog>> value) throws Exception {
                        return value.f1;
                    }
                })
                .filter(t -> t.f0 != null)
                .keyBy(t -> t.f0);
        SingleOutputStreamOperator<SecurityEventLog> domainProcess = cnRecordSource
                .filter((log) -> log.getDomain() != null)
                .keyBy(CnRecordLog::getDomain)
                .connect(domainDataStream)
                .process(new DomainWindowFunction())
                .setParallelism(CommonConfig.SECURITY_DOMAIN_PARALLELISM);

        KeyedStream<Tuple2<String, IocJsonLog>, String> urlDataStream = iocData
                .map(new MapFunction<Tuple3<Tuple2<String, IocJsonLog>, Tuple2<String, IocJsonLog>, Tuple2<String, IocJsonLog>>, Tuple2<String, IocJsonLog>>() {
                    @Override
                    public Tuple2<String, IocJsonLog> map(Tuple3<Tuple2<String, IocJsonLog>, Tuple2<String, IocJsonLog>, Tuple2<String, IocJsonLog>> value) throws Exception {
                        return value.f2;
                    }
                })
                .filter(t -> t.f0 != null).keyBy(t -> t.f0);
        SingleOutputStreamOperator<SecurityEventLog> urlProcess = cnRecordSource
                .filter((log) -> log.getHttp_url() != null)
                .keyBy(CnRecordLog::getHttp_url)
                .connect(urlDataStream)
                .process(new UrlWindowFunction())
                .setParallelism(CommonConfig.SECURITY_URL_PARALLELISM);

        DataStream<SecurityEventLog> resultDataStream = ipProcess.union(domainProcess).union(urlProcess);
        resultDataStream.print();
        List<HttpHost> esAddresses = ESSinkUtils.getEsAddresses(com.zdjizhi.base.common.CommonConfig.ES_HOST);
        ESSinkUtils.addSink(esAddresses, CommonConfig.ES_MAX_FLUSH, resultDataStream, new EsSinkFunctionImpl());

    }


    public static void main(String[] args) throws Exception {
        new CnSecurityEvent().schedule();
    }
}
