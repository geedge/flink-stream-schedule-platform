package com.zdjizhi.base.rule.info;

import com.alibaba.fastjson2.annotation.JSONField;
import lombok.Data;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonValue;

import java.time.Duration;
import java.util.Locale;
import java.util.Map;

/**
 * The Rule Information about a rule.
 *
 * @author chaoc
 * @since 1.0
 */
@Data
public class RuleInfo {

    private long ruleId;
    private RuleType ruleType;
    private String name;
    private String category;
    private String eventType;
    // Whether the rule is built-in (true) or custom (false).
    private boolean builtIn;
    // Whether the current rule is enabled (true) or disabled (false).
    private boolean status;
    private RuleConfig ruleConfig;
    private Trigger trigger;

    // ===================================================================
    //                         Field type class
    // ===================================================================

    /**
     * Config detail of rule.
     */
    public interface RuleConfig {
    }

    @Data
    public static final class IndicatorMatchConfig
            implements RuleConfig {
        private String dataSource;
        private Integer libraryId;
        private String libraryName;
        private String libraryCategory;
        private String librarySource;
    }

    @Data
    public static final class ThresholdConfig
            implements RuleConfig {
        private String dataSource;
        private String dimensionKeys;
        private String filters;
        private Map<Level, String> conditions;
    }

    /**
     * Type of rule.
     */
    public enum RuleType {

        INDICATOR_MATCH("indicator_match"),
        THRESHOLD("threshold");

        private final String value;

        RuleType(final String value) {
            this.value = value;
        }

        @JsonValue
        public String getValue() {
            return value.toLowerCase(Locale.ROOT);
        }

        @JsonCreator
        public static RuleType of(final String v) {
            for (RuleType type : values()) {
                if (type.value.equalsIgnoreCase(v)) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Invalid rule type: " + v);
        }
    }

    /**
     * The Trigger information about rule triggering conditions.
     */
    @Data
    public static final class Trigger {
        private int atLeast;
        @JsonProperty("interval")
        private String duration;
        private String resetInterval;
    }

    /**
     * The Level enum represents different levels of rule severity.
     */
    public enum Level {
        CRITICAL,
        HIGH,
        MEDIUM,
        LOW,
        INFO;

        @JsonValue
        public String getName() {
            return this.name().toLowerCase(Locale.ROOT);
        }

        @JsonCreator
        public static Level of(final String name) {
            for (Level value : values()) {
                if (value.name().equalsIgnoreCase(name)) {
                    return value;
                }
            }
            throw new IllegalArgumentException("Invalid rule level name: " + name);
        }
    }
}
