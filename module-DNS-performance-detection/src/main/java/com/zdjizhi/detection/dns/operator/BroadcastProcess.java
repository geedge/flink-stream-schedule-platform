package com.zdjizhi.detection.dns.operator;

import com.zdjizhi.base.common.CnRecordLog;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wlh
 */
public class BroadcastProcess extends ProcessAllWindowFunction<CnRecordLog, Tuple3<Integer,Integer, Double>,TimeWindow> {

    private static Logger LOG = LoggerFactory.getLogger(BroadcastProcess.class);

    @Override
    public void process(Context context, Iterable<CnRecordLog> elements, Collector<Tuple3<Integer,Integer, Double>> out) {
        try {
            int dnsCnt = 0;
            int httpCnt = 0;
            double median = 0d;
            for (CnRecordLog ignored : elements) {
                if ("DNS".equals(ignored.getCommon_schema_type())) {
                    dnsCnt++;
                }
                if ("HTTP".equals(ignored.getCommon_schema_type())){
                    httpCnt++;
                }
            }
            out.collect(Tuple3.of(dnsCnt,httpCnt, median));
        }catch (Exception e){
            LOG.error("计算 broadcast 失败",e);
        }
    }

}
