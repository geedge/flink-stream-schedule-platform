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
        setDomainAndDomainSld(recordLog);
        setl4Protocol(recordLog);
        setl7ProtocolAndApp(recordLog);
    }

    private static void setDomainAndDomainSld(CnRecordLog cnRecordLog) {
        cnRecordLog.setDomain(cnRecordLog.getServer_fqdn());
        cnRecordLog.setDomain_sld(cnRecordLog.getServer_domain());
    }

    private static void setl4Protocol(CnRecordLog cnRecordLog) {
        String protocolPath = cnRecordLog.getProtocol_path();
        if (protocolPath != null) {
            String[] protocols = cnRecordLog.getProtocol_path().split("\\.");
            if (protocols.length > 0) {
                String l4Protocol = protocols[protocols.length - 1];
                cnRecordLog.setCommon_l4_protocol(l4Protocol);
            }
        }
    }

    private static void setl7ProtocolAndApp(CnRecordLog cnRecordLog) {
        try {
            String appFullPath = cnRecordLog.getCommon_app_full_path();
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
            cnRecordLog.setCommon_l7_protocol(l7Protocol);
            cnRecordLog.setCommon_app_label(app);
        } catch (Exception e) {
            LOG.error("ETL解析错误\nrecordLog:{} \n", cnRecordLog, e);
        }
    }
}

