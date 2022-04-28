package com.zdjizhi.detection.dns.operator;

import com.zdjizhi.base.common.CnRecordLog;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wlh
 */
public class BroadcastProcess extends ProcessWindowFunction<CnRecordLog,Tuple2<Long,Long>,Long,TimeWindow> {

    private static Logger LOG = LoggerFactory.getLogger(BroadcastProcess.class);

    @Override
    public void process(Long aLong, Context context, Iterable<CnRecordLog> elements, Collector<Tuple2<Long, Long>> out) {
        try {
            long dnsCnt = 0;
            long httpCnt = 0;
            for (CnRecordLog ignored : elements) {
                if ("DNS".equals(ignored.getCommon_schema_type())) {
                    dnsCnt++;
                }
                if ("HTTP".equals(ignored.getCommon_schema_type())){
                    httpCnt++;
                }
            }
            out.collect(Tuple2.of(dnsCnt,httpCnt));
        }catch (Exception e){
            LOG.error("计算 broadcast 失败",e);
        }
    }

}
