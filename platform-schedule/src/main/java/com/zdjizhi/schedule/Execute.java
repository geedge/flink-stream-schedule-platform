package com.zdjizhi.schedule;

import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.platform.Schedule;
import com.zdjizhi.base.utils.FlinkEnvironmentUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Execute {
    private static final Logger logger = LoggerFactory.getLogger(Execute.class);

    public static void main(String[] args) throws Exception {
        execute();
    }

    private static void execute() throws Exception {
        try {
            Properties propService = new Properties();
            propService.load(Execute.class.getClassLoader().getResourceAsStream("business.properties"));
            if (!propService.isEmpty()){
                for (Object key : propService.keySet()) {
                    String className = propService.getProperty(key.toString());
                    Class cls = Class.forName(className);
                    Schedule schedule = (Schedule) cls.newInstance();
                    schedule.schedule();
                    logger.warn("{} 已加载",className);
                }
                FlinkEnvironmentUtils.streamExeEnv.execute(CommonConfig.STREAM_EXECUTION_JOB_NAME);
            }
        }catch (Exception e){
            logger.error("任务调度异常",e);
            System.exit(1);
        }
    }

}
