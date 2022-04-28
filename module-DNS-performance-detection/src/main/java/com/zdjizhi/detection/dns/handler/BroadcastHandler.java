package com.zdjizhi.detection.dns.handler;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.detection.dns.common.CommonConfig;
import com.zdjizhi.detection.dns.operator.BroadcastProcess;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class BroadcastHandler {

    public static BroadcastStream<Tuple2<Long,Long>> getBroadcast(SingleOutputStreamOperator<CnRecordLog> cnRecordSource){

        SingleOutputStreamOperator<Tuple2<Long, Long>> broadcastNum = cnRecordSource
//                .keyBy(log -> DateUtils.getTimeFloor(new Date(log.getCommon_recv_time() * 1000L), "PT10S").getTime() / 1000)
                .keyBy(log -> log.getCommon_recv_time() % CommonConfig.PERFORMANCE_DETECTION_PARALLELISM)
                .window(TumblingEventTimeWindows.of(Time.seconds(CommonConfig.FLINK_WINDOW_MAX_TIME)))
                .process(new BroadcastProcess())
                .setParallelism(CommonConfig.PERFORMANCE_DETECTION_PARALLELISM)
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(CommonConfig.FLINK_WINDOW_MAX_TIME)))
                .reduce((ReduceFunction<Tuple2<Long, Long>>) (value1, value2) -> Tuple2.of(value1.f0 + value2.f0, value1.f1 + value2.f1));

        broadcastNum.print();

        MapStateDescriptor<Void, Tuple2<Integer,Integer>> bcStateDescriptor =
                new MapStateDescriptor<>("broadcast", Types.VOID, Types.TUPLE(Types.LONG,Types.LONG));

        return broadcastNum.broadcast(bcStateDescriptor);
    }

    public static void main(String[] args) {
        long x = 20L % 2;
        System.out.println(x);
    }

}
