package com.zdjizhi.schedule.indicator.record;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Grouping structure for Indicator match.
 *
 * @author chaoc
 * @since 23.10
 */
@Data
@AllArgsConstructor
public class MatchGroup {

    private long ruleId;

    private String clientIp;

    private String serverIp;

    @Override
    public String toString() {
        return String.join(",",
                ruleId + "", clientIp, serverIp);
    }
}
