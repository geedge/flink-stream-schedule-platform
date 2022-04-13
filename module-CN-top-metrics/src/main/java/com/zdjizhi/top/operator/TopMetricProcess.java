package com.zdjizhi.top.operator;

import com.zdjizhi.top.common.CommonConfig;
import com.zdjizhi.top.common.TopBaseLog;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.TreeSet;

public class TopMetricProcess extends ProcessWindowFunction<TopBaseLog,TopBaseLog,Long, TimeWindow> {

    private String groupBy;
    private String orderBy;

    public TopMetricProcess(String groupBy,String orderBy) {
        this.groupBy = groupBy;
        this.orderBy = orderBy;
    }

    @Override
    public void process(Long s, Context context, Iterable<TopBaseLog> elements, Collector<TopBaseLog> out) throws Exception {
        TreeSet<TopBaseLog> baseLogs = new TreeSet<>(new Comparator<TopBaseLog>() {
            @Override
            public int compare(TopBaseLog o1, TopBaseLog o2) {
                return (Double.compare(o1.get(orderBy).doubleValue(), o2.get(orderBy).doubleValue()));
            }
        });

        for (TopBaseLog topBaseLog:elements){
            topBaseLog.setGroup_by(groupBy);
            topBaseLog.setOrder_by(orderBy);
            if (baseLogs.size() > CommonConfig.TOP_METRICS_ROWNUM_LIMIT){
                TopBaseLog last = baseLogs.last();
                if (Double.compare(topBaseLog.get(orderBy).doubleValue(), last.get(orderBy).doubleValue()) > 0){
                    baseLogs.pollLast();
                    baseLogs.add(topBaseLog);
                }
            }else {
                baseLogs.add(topBaseLog);
            }
        }

        for (TopBaseLog topBaseLog:baseLogs){
            out.collect(topBaseLog);
        }

    }
}
