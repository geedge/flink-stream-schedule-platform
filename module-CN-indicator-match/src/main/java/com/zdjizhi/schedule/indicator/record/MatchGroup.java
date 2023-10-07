package com.zdjizhi.schedule.indicator.record;

import lombok.Data;

/**
 * Grouping structure for Indicator match.
 *
 * @author chaoc
 * @since 23.10
 */
@Data
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
