package com.zdjizhi.schedule.indicator.functions;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.schedule.indicator.record.MatchGroup;
import com.zdjizhi.schedule.indicator.record.SlideAggregate;
import org.apache.flink.api.java.functions.KeySelector;


/**
 * Selecting a key for aggregated records.
 *
 * @author chaoc
 * @since 23.10
 */
public class SlidedKeySelector implements KeySelector<SlideAggregate<CnRecordLog>, MatchGroup> {

    private final MatchKeySelector keySelector;

    public SlidedKeySelector(final MatchKeySelector keySelector) {
        this.keySelector = keySelector;
    }

    @Override
    public MatchGroup getKey(SlideAggregate<CnRecordLog> value) throws Exception {
        return this.keySelector.getKey(value.getValue());
    }
}
