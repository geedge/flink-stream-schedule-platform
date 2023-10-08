package com.zdjizhi.schedule.indicator.functions;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.schedule.indicator.record.MatchGroup;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * Selecting a key for matching records.
 * @author chaoc
 * @since 23.10
 */
public class MatchKeySelector
        implements KeySelector<CnRecordLog, MatchGroup> {

    @Override
    public MatchGroup getKey(CnRecordLog log) throws Exception {
        return new MatchGroup(
                log.getRule_id_list().get(0),
                log.getCommon_server_ip(),
                log.getCommon_client_ip());
    }
}
