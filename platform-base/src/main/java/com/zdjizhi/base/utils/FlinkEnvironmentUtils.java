package com.zdjizhi.base.utils;

import com.zdjizhi.base.common.CommonConfig;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;


/**
 * @author wlh
 */
public class FlinkEnvironmentUtils {
    public static StreamExecutionEnvironment streamExeEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    private static StreamTableEnvironment tableEnvironment;

    static {
        streamExeEnv.setParallelism(CommonConfig.STREAM_EXECUTION_ENVIRONMENT_PARALLELISM);
        //checkpoint配置
        streamExeEnv.enableCheckpointing(CommonConfig.FLINK_ENABLE_CHECKPOINT);
        CheckpointConfig checkpointConfig = streamExeEnv.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setMinPauseBetweenCheckpoints(CommonConfig.FLINK_MIN_PAUSE_BETWEEN_CHECKPOINTS);
        checkpointConfig.setCheckpointTimeout(CommonConfig.FLINK_CHECKPOINT_TIMEOUT);
        checkpointConfig.setTolerableCheckpointFailureNumber(CommonConfig.FLINK_TOLERABLE_CHECKPOINT_FAILURE_NUMBER);
        checkpointConfig.setMaxConcurrentCheckpoints(CommonConfig.FLINK_MAX_CONCURRENT_CHECKPOINT);
        checkpointConfig.enableUnalignedCheckpoints();
    }

    public static <T> WatermarkStrategy<T> createWatermarkStrategy(Duration maxOutOfOrderness, SerializableTimestampAssigner<T> timestampAssigner){
        return WatermarkStrategy
                .<T>forBoundedOutOfOrderness(maxOutOfOrderness)
                .withTimestampAssigner(timestampAssigner);
    }

    public static StreamTableEnvironment getTableEnv(){
        if (tableEnvironment == null ){
            tableEnvironment = StreamTableEnvironment.create(streamExeEnv);
        }
        return tableEnvironment;
    }

}
