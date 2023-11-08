package com.zdjizhi.schedule;

import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.config.Configs;
import com.zdjizhi.base.platform.Schedule;
import com.zdjizhi.base.utils.FlinkEnvironmentUtils;
import com.zdjizhi.etl.CnRecordEtl;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class Execute {
    private static final Logger logger = LoggerFactory.getLogger(Execute.class);

    public static void main(String[] args) throws IOException {
        loadConfiguration(args);
        execute();
    }

    private static void loadConfiguration(String[] args) throws IOException {
        Configuration configuration = Configs.create(args);
        FlinkEnvironmentUtils.streamExeEnv.getConfig().setGlobalJobParameters(configuration);
    }

    private static void execute(){
        try {
            CnRecordEtl cnRecordEtl = new CnRecordEtl();
            Map<String, String> pluginMap = Configs.getPluginConfig().toMap();
            for (Object key : pluginMap.keySet()) {
                String className = pluginMap.get(key.toString());
                Class cls = Class.forName(className);
                Schedule schedule = (Schedule) cls.newInstance();
                schedule.schedule();
                logger.info("{} 已加载",className);
            }
            FlinkEnvironmentUtils.streamExeEnv.execute(Configs.get(CommonConfig.STREAM_EXECUTION_JOB_NAME));
        }catch (Exception e){
            e.printStackTrace(System.out);
            throw new RuntimeException("任务调度异常", e);
        }
    }
}
