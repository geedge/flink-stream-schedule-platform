package com.zdjizhi.base.utils;

import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.config.Configs;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
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
    public static StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(streamExeEnv);

    static {
        streamExeEnv.setParallelism(Configs.get(CommonConfig.STREAM_EXECUTION_ENVIRONMENT_PARALLELISM));

        streamExeEnv.getConfig().enableObjectReuse();

        //checkpoint配置
        switch (Configs.get(CommonConfig.FLINK_ENABLE_CHECKPOINT_FLAG)) {
            case "0":
                break;
            case "1":
                streamExeEnv.enableCheckpointing(Configs.get(CommonConfig.FLINK_ENABLE_CHECKPOINT_TIME) * 1000L);
                CheckpointConfig checkpointConfig = streamExeEnv.getCheckpointConfig();
                checkpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
                checkpointConfig.setMinPauseBetweenCheckpoints(Configs.get(CommonConfig.FLINK_MIN_PAUSE_BETWEEN_CHECKPOINTS) * 1000L);
                checkpointConfig.setCheckpointTimeout(Configs.get(CommonConfig.FLINK_CHECKPOINT_TIMEOUT) * 1000L);
                checkpointConfig.setTolerableCheckpointFailureNumber(Configs.get(CommonConfig.FLINK_TOLERABLE_CHECKPOINT_FAILURE_NUMBER));
                checkpointConfig.setMaxConcurrentCheckpoints(Configs.get(CommonConfig.FLINK_MAX_CONCURRENT_CHECKPOINT));
                checkpointConfig.enableUnalignedCheckpoints();
                checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
                break;
            default:
                throw new IllegalArgumentException();
        }
    }

    public static <T> WatermarkStrategy<T> createWatermarkStrategy(Duration maxOutOfOrderness, SerializableTimestampAssigner<T> timestampAssigner) {
        return WatermarkStrategy
                .<T>forBoundedOutOfOrderness(maxOutOfOrderness)
                .withTimestampAssigner(timestampAssigner);
    }

    /*
    public static StreamTableEnvironment getTableEnv() {
        if (tableEnvironment == null) {
            tableEnvironment = StreamTableEnvironment.create(streamExeEnv);
        }
        return tableEnvironment;
    }
    */

}
