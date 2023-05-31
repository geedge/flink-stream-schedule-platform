package com.zdjizhi.etl.broadcast;

import com.alibaba.fastjson2.JSON;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.etl.reader.CsvReader;
import com.zdjizhi.etl.utils.*;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class BroadcastProcessFunc extends BroadcastProcessFunction<String, Map<String, byte[]>, CnRecordLog> {
    private static final Logger LOG = LoggerFactory.getLogger(BroadcastProcessFunc.class);
    private static MapStateDescriptor<String,Map> descriptor =
            new MapStateDescriptor<>("descriptorTest", Types.STRING, TypeInformation.of(Map.class));

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        System.out.println("begin init");

        IPUtils.loadIpLook();

        CsvReader csvReader = new CsvReader();
        csvReader.loadCsv();

        System.out.println("init over");
    }

    @Override
    public void processElement(String source, ReadOnlyContext ctx, Collector<CnRecordLog> out) throws Exception {
        CnRecordLog recordLog = new CnRecordLog();
        try {
            recordLog = JSON.parseObject(source, CnRecordLog.class);
            CompletedUtils.setCompletedMessage(recordLog);
            IPUtils.setMmdbMessage(recordLog);
            IdcRenterUtils.setIdcRenter(recordLog);
            LinkUtils.setLinkMessage(recordLog);
            FcUtils.setWebSketchMessage(recordLog);
            FcUtils.setIcpMessage(recordLog);
            WhoisUtils.setWhoisSketchMessage(recordLog);
            DnsServerUtils.setWebSktMessage(recordLog);
            AppUtils.setAppSketchMessage(recordLog);
            IPUtils.setInternalIpFlag(recordLog);
            TagUtils.setTag(recordLog);
            out.collect(recordLog);
        } catch (Exception e) {
            LOG.error("ETL解析错误\nsource:{} \nrecordLog:{} \n", source, recordLog, e);
        }
    }

    @Override
    public void processBroadcastElement(Map<String, byte[]> value, Context ctx, Collector<CnRecordLog> out) throws Exception {
        try {
            System.out.println("接收到广播数据列表：" + String.join(",", value.keySet()));
            BroadcastState<String, Map> broadcastState = ctx.getBroadcastState(descriptor);
            broadcastState.clear();
            broadcastState.put("knowledge", value);

            IPUtils.updateIpLook(value);
            TagUtils.updateTag(value);

            CsvReader csvReader = new CsvReader();
            csvReader.updateCsv(value);

            System.out.println("广播数据更新完成");
        } catch (Exception e) {
            LOG.error("加载文件失败", e);
        }
    }
}


