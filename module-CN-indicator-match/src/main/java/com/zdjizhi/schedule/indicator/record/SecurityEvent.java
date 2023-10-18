package com.zdjizhi.schedule.indicator.record;

import com.alibaba.fastjson2.annotation.JSONField;
import com.zdjizhi.base.rule.info.RuleInfo;
import lombok.Data;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonValue;

/**
 * Security Event Pojo Mapping
 *
 * @author chaoc
 * @since 23.10
 */
@Data
public class SecurityEvent {

    @JSONField(name = "event_id")
    @JsonProperty("event_id")
    private long eventId;

    @JSONField(name = "event_type")
    @JsonProperty("event_type")
    private String eventType;

    @JSONField(name = "event_name")
    @JsonProperty("event_name")
    private String eventName;

    @JSONField(name = "event_key")
    @JsonProperty("event_key")
    private String eventKey;

    @JSONField(name = "rule_id")
    @JsonProperty("rule_id")
    private long ruleId;

    @JSONField(name = "rule_type")
    @JsonProperty("rule_type")
    private RuleInfo.RuleType ruleType;

    @JSONField(serialize = false)
    @JsonIgnore
    private boolean builtin;

    private RuleInfo.Level severity;

    @JSONField(name = "offender_ip")
    @JsonProperty("offender_ip")
    private String offenderIp;

    @JSONField(name = "victim_ip")
    @JsonProperty("victim_ip")
    private String victimIp;

    private String domain;

    private String app;

    @JSONField(name = "start_time")
    @JsonProperty("start_time")
    private int startTime;

    @JSONField(name = "end_time")
    @JsonProperty("end_time")
    private Integer endTime;

    @JSONField(name = "duration_s")
    @JsonProperty("duration_s")
    private int durationS;

    @JSONField(name = "match_times")
    @JsonProperty("match_times")
    private int matchTimes;

    private EventStatus status;

    @JSONField(name = "event_info")
    @JsonProperty("event_info")
    private String eventInfo;

    @JSONField(name = "is_builtin")
    @JsonProperty("is_builtin")
    public int getBuiltin() {
        return builtin ? 1 : 0;
    }

    public enum EventStatus {

        ON_GOING(0),
        ENDED(1);

        private final int code;

        EventStatus(final int code) {
            this.code = code;
        }

        @JSONField
        @JsonValue
        public final int getCode() {
            return code;
        }
    }

    @Data
    public static final class EventInfo {

        private long libraryId;
        private String name;
        private String iocType;
        private String iocValue;

        @Override
        public String toString() {
            return "{" +
                    "\"knowledge_id\":\"" + libraryId + "\"," +
                    "\"name\":\"" + name + "\"," +
                    "\"ioc_type\":\"" + iocType + "\"," +
                    "\"ioc_value\":\"" + iocValue + "\"" +
                    "}";
        }
    }
}
