package com.zdjizhi.schedule.indicator.functions;

import com.zdjizhi.base.common.CnRecordLog;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.Collections;

/**
 * Flatting records by rule-id
 *
 * @author chaoc
 * @since 23.10
 */
public class RuleIDFlatMapFunction extends RichFlatMapFunction<CnRecordLog, CnRecordLog> {

    @Override
    public void flatMap(CnRecordLog log, Collector<CnRecordLog> out) throws Exception {
        for (final Long ruleId : log.getRule_id_list()) {
            log.setRule_id_list(Collections.singletonList(ruleId));
            out.collect(log);
        }
    }
}
