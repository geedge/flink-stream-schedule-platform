package com.zdjizhi.etl.utils;

import com.zdjizhi.base.common.CnRecordLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @ Author pengfeixu
 * @ Date 2022/3/8
 * @ Description
 */
public class CompletedUtils {

    private static final Logger LOG = LoggerFactory.getLogger(CompletedUtils.class);

    private final static AtomicBoolean IS_INIT = new AtomicBoolean(false);
    private final static CountDownLatch ASYNC_WAIT = new CountDownLatch(1);

    private static HashMap<String, String> l7ProtocolMap = new HashMap<>();

    public static void init(String l7Protocols) throws InterruptedException {
        if (IS_INIT.compareAndSet(false, true)) {
            l7ProtocolMap = Arrays.stream(l7Protocols.split(",")).collect(Collectors.toMap(String::toLowerCase, Function.identity(), (existing, replacement) -> existing, HashMap::new));
            ASYNC_WAIT.countDown();
        }
        ASYNC_WAIT.await();
    }

    public static void setCompletedMessage(CnRecordLog recordLog) {
        try {
            String appFullPath = recordLog.getCommon_app_full_path();
            String l7Protocol = "UNCATEGORIZED";
            String app = "";
            if (appFullPath != null) {
                appFullPath = appFullPath.toLowerCase();
                String[] split = appFullPath.split("\\.");
                int i = 0;
                boolean protocolend = false;
                while (i < split.length) {
                    String s = split[i];
                    boolean isProtocol = false;
                    if (l7ProtocolMap.containsKey(s)) {
                        isProtocol = true;
                        if (!protocolend) {
                            l7Protocol = l7ProtocolMap.get(s);
                        }
                    }
                    if (!isProtocol) {
                        protocolend = true;
                        app = s;
                    }
                    i++;
                }
            }
            recordLog.setCommon_l7_protocol(l7Protocol);
            recordLog.setCommon_app_label(app);
        } catch (Exception e) {
            LOG.error("ETL解析错误\nrecordLog:{} \n", recordLog, e);
        }
    }
}

