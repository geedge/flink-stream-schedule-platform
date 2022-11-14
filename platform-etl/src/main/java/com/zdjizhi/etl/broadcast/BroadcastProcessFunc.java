package com.zdjizhi.etl.broadcast;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.common.CustomFile;
import com.zdjizhi.etl.reader.CsvReader;
import com.zdjizhi.etl.reader.IpLookReader;
import com.zdjizhi.etl.reader.JsonReader;
import com.zdjizhi.etl.utils.*;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class BroadcastProcessFunc extends BroadcastProcessFunction<String, List<CustomFile>, CnRecordLog> {
    private String clusterOrSingle;
    private static IpLookReader ipLookup;
    private static final Logger LOG = LoggerFactory.getLogger(BroadcastProcessFunc.class);
    private static JsonMapper jsonMapperInstance = JsonMapper.getInstance();

    MapStateDescriptor<String,List<CustomFile>> descriptor ;
//            new MapStateDescriptor<String, List<CustomFile>>("descriptorTest", Types.STRING, Types.LIST(TypeInformation.of(CustomFile.class)));
    public BroadcastProcessFunc( MapStateDescriptor descriptor,String clusterOrSingle) {
        this.descriptor=descriptor;
        this.clusterOrSingle=clusterOrSingle;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        System.out.println("begin init");
        ipLookup=new IpLookReader(CommonConfig.CLUSTER_OR_SINGLE);
        ipLookup.loadIpLook();

        CsvReader csvReader = new CsvReader(clusterOrSingle);
        JsonReader jsonReader = new JsonReader(clusterOrSingle);
        csvReader.loadCsv();
        jsonReader.loadJson();
        System.out.println("init over");
    }

    @Override
    public void processElement(String source, ReadOnlyContext ctx, Collector<CnRecordLog> out) throws Exception {
        CnRecordLog recordLog = new CnRecordLog();
        try {
            recordLog = jsonMapperInstance.fromJson(source, CnRecordLog.class);

            CompletedUtils.setCompletedMessage(recordLog);
            if ((ipLookup.getAsn(recordLog.getCommon_client_ip())!=null)&& (ipLookup.getAsn(recordLog.getCommon_server_ip())!=null)){
                IPUtils.setMmdbMessage(recordLog,ipLookup.ipLookDetail(recordLog.getCommon_client_ip()).toString(),ipLookup.getAsn(recordLog.getCommon_client_ip()).toString(),"client");
                IPUtils.setMmdbMessage(recordLog,ipLookup.ipLookDetail(recordLog.getCommon_server_ip()).toString(),ipLookup.getAsn(recordLog.getCommon_server_ip()).toString(),"server");

            }
            IdcRenterUtils.setIdcRenter(recordLog);
            LinkUtils.setLinkMessage(recordLog);
            IspUtils.setIspMessage(recordLog);
            FcUtils.setWebSketchMessage(recordLog);
            FcUtils.setIcpMessage(recordLog);
            WhoisUtils.setWhoisSketchMessage(recordLog);
            DnsServerUtils.setWebSktMessage(recordLog);
            AppUtils.setAppSketchMessage(recordLog);
            IPUtils.setInternalIpFlag(recordLog);
            out.collect(recordLog);
        } catch (Exception e) {
            LOG.error("ETL解析错误\nsource:{} \nrecordLog:{} \n", source, recordLog, e);
        }
    }

    @Override
    public void processBroadcastElement(List<CustomFile> value, Context ctx, Collector<CnRecordLog> out) throws Exception {
        try {
            BroadcastState<String, List<CustomFile>> broadcastState = ctx.getBroadcastState(descriptor);
            broadcastState.clear();
            broadcastState.put("knowledge", value);
//            ipLookup=new IpLookReader(CommonConfig.CLUSTER_OR_SINGLE);
//            ipLookup.updateIpLook(value);
            CsvReader csvReader = new CsvReader(clusterOrSingle);
            JsonReader jsonReader = new JsonReader(clusterOrSingle);
            csvReader.updateCsv(value);
            jsonReader.updateJson(value);
        }catch (Exception e){
            LOG.error("加载文件失败", e);
        }

    }


    }


