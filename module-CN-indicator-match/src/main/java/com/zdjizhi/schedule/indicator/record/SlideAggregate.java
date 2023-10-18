package com.zdjizhi.schedule.indicator.record;


import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * An aggregation result for sliding window operations.
 *
 * @param <T> The type of value to be aggregated.
 * @author chaoc
 * @since 23.10
 */
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class SlideAggregate<T> {

    @Setter
    private T value;

    private int times;

    private long firstTime;

    private long lastTime;

    /**
     * Increment the count of times this value has been aggregated.
     */
    public void incTimes() {
        this.times++;
    }

    /**
     * Update the timestamp information for this value.
     *
     * @param time The timestamp to update with.
     */
    public void updateTime(long time) {
        this.firstTime = min(this.firstTime, time);
        this.lastTime = max(this.lastTime, time);
    }
}
