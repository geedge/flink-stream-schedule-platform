package com.zdjizhi.schedule.indicator.functions;

import com.zdjizhi.base.common.CnRecordLog;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Flatting records by rule-id
 *
 * @author chaoc
 * @since 23.10
 */
public class RuleIDFlatMapFunction extends RichFlatMapFunction<CnRecordLog, CnRecordLog> {

    @Override
    public void flatMap(CnRecordLog log, Collector<CnRecordLog> out) throws Exception {
        final Set<Long> set = new HashSet<>();
        for (int i = 0; i < log.getRule_id_list().size(); i++) {
            final Long ruleId = log.getRule_id_list().get(i);
            // Remove duplicate elements from the Rule List
            if (set.contains(ruleId)) {
                break;
            } else {
                set.add(ruleId);
            }
            log.setRule_id_list(Collections.singletonList(ruleId));
            log.setIoc_type_list(Collections.singletonList(log.getIoc_type_list().get(i)));
            out.collect(log);
        }
    }
}
