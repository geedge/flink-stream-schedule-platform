package com.zdjizhi.base.rule.info;

import lombok.Data;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonValue;

import java.util.Locale;

/**
 * The Rule Information about a rule.
 *
 * @author chaoc
 * @since 1.0
 */
@Data
public class RuleInfo {

    // TODO field list.

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
