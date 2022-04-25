package com.zdjizhi.detection.dns.handler;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.detection.dns.common.CommonConfig;
import com.zdjizhi.detection.dns.operator.BroadcastProcess;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class BroadcastHandler {

    public static BroadcastStream<Tuple2<Integer,Integer>> getBroadcast(SingleOutputStreamOperator<CnRecordLog> cnRecordSource){
        SingleOutputStreamOperator<Tuple2<Integer,Integer>> broadcastMedianNum = cnRecordSource
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(CommonConfig.FLINK_WINDOW_MAX_TIME)))
                .process(new BroadcastProcess())
//                .slotSharingGroup(String.valueOf(cnRecordSource.getId()))
                ;

        MapStateDescriptor<Void, Tuple2<Integer,Integer>> bcStateDescriptor =
                new MapStateDescriptor<>("broadcast", Types.VOID, Types.TUPLE(Types.INT,Types.DOUBLE));

        return broadcastMedianNum.broadcast(bcStateDescriptor);
    }

}
