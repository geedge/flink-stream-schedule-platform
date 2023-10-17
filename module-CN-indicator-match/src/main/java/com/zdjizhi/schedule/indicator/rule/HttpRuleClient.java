package com.zdjizhi.schedule.indicator.rule;

import com.zdjizhi.base.rule.RuleClient;
import com.zdjizhi.base.rule.info.RuleInfo;
import com.zdjizhi.schedule.indicator.common.Configs;
import lombok.Data;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.ObjectCodec;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.*;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Get rule information from a remote HTTP rule service.
 *
 * @author chaoc
 * @since 23.10
 */
public class HttpRuleClient implements RuleClient<Long, RuleInfo> {

    private static final Logger LOG = LoggerFactory.getLogger(HttpRuleClient.class);

    private final HttpClient client;

    private final AtomicLong currentVersion;

    private final ObjectMapper mapper;

    private final String fullUrl;

    private final String incUrlFormat;

    /**
     * Constructs a new HttpRuleClient.
     */
    public HttpRuleClient() {
        this(Holder.HTTP_CLIENT);
    }

    /**
     * Constructs a new HttpRuleClient.
     *
     * @param client Http Client
     */
    public HttpRuleClient(final HttpClient client) {
        this(
                client,
                new AtomicLong(-1),
                new ObjectMapper(),
                Configs.create().get(Configs.RULE_FULL_URL),
                Configs.create().get(Configs.RULE_INC_URL) + "/%d");
    }

    HttpRuleClient(
            final HttpClient client,
            final AtomicLong currentVersion,
            final ObjectMapper mapper,
            final String fullUrl,
            final String incUrlFormat) {
        this.client = client;
        this.currentVersion = currentVersion;
        this.mapper = mapper;
        this.fullUrl = fullUrl;
        this.incUrlFormat = incUrlFormat;
        final SimpleModule ruleConfigModule = new SimpleModule();
        ruleConfigModule.addDeserializer(RuleInfo.RuleConfig.class, new RuleConfigJsonDeserializer());
        this.mapper.registerModule(ruleConfigModule);
        this.mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public Updatable<Long, RuleInfo> get() {
        try {
            final String content = httpGet(getURL());

            final RuleResponse wrapper =
                    mapper.readValue(content, RuleResponse.class);
            final DataValue data = wrapper.getData();
            currentVersion.getAndAccumulate(data.version,
                    (currentValue, updateValue) -> Math.max(updateValue, currentValue));

            return new SimpleUpdatable<>(
                    nullSafeList(data.getAddList()),
                    nullSafeList(data.getUpdateList()),
                    nullSafeList(data.getDeleteIds()));
        } catch (IOException e) {
            LOG.info("Rule Info parser error.", e);
            // Handle error returning an empty Updatable.
            return new SimpleUpdatable<>(Collections.emptyList(),
                    Collections.emptyList(), Collections.emptyList());
        }
    }


    // ======================================================================================
    // ----------------------------------- private helper -----------------------------------

    private static final class Holder {
        private final static HttpClient HTTP_CLIENT = HttpClients.createMinimal();
    }

    private String httpGet(final String url) throws IOException {
        final HttpGet httpGet = new HttpGet(url);
        final HttpResponse response = client.execute(doGet(httpGet));
        return EntityUtils.toString(response.getEntity(), "UTF-8");
    }

    private String getURL() {
        if (currentVersion.get() > 0) {
            return String.format(incUrlFormat, currentVersion.get());
        } else {
            return fullUrl;
        }
    }

    private HttpGet doGet(final HttpGet httpGet) {
        httpGet.addHeader("Accept", "application/json");
        httpGet.addHeader("Cn-Authorization", Configs.create().get(Configs.RULE_API_TOKEN));
        return httpGet;
    }

    private <T> List<T> nullSafeList(@Nullable List<T> list) {
        return list == null ? Collections.emptyList() : list;
    }

    // ============================================================
    //                Data wrapper For Json parser
    // ============================================================

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static final class RuleResponse {
        private Integer code;
        private String msg;
        private DataValue data;
    }

    @Data
    private static final class DataValue {
        private Integer version;
        private List<RuleInfoJsonWrapper> addList;
        private List<RuleInfoJsonWrapper> updateList;
        @JsonProperty("deleteList")
        private List<Long> deleteIds;
    }

    @Data
    private static final class Rules {
        private List<RuleInfoJsonWrapper> addList;
        private List<RuleInfoJsonWrapper> updateList;
        private List<Long> deleteIds;
    }

    private static final class RuleInfoJsonWrapper extends RuleInfo {

        public void setIsBuiltIn(int isBuiltIn) {
            setBuiltIn(isBuiltIn == 1);
        }

        public void setStatus(int status) {
            setEnabled(status == 1);
        }

        @JsonIgnore
        @Override
        public void setTrigger(Trigger trigger) {
            super.setTrigger(trigger);
        }

        @JsonProperty("ruleTriggerObj")
        public void setTriggerWrapper(final TriggerWrapper wrapper) {
            setTrigger(wrapper);
        }

        @JsonProperty("ruleConfigObj")
        @Override
        public void setRuleConfig(RuleConfig ruleConfig) {
            super.setRuleConfig(ruleConfig);
        }
    }

    private static final class IndicatorConfigWrapper
            extends RuleInfo.IndicatorMatchConfig {

        @JsonProperty("knowledgeId")
        @Override
        public void setLibraryId(Long libraryId) {
            super.setLibraryId(libraryId);
        }

        @JsonProperty("knowledgeBase")
        public void setLibrary(final LibraryWrapper wrapper) {
            setLibraryId(wrapper.getId());
            setLibraryName(wrapper.getName());
            setLibraryCategory(wrapper.getCategory());
            setLibrarySource(wrapper.getSource());
        }
    }

    @Data
    private static final class LibraryWrapper {
        @JsonProperty("knowledgeId")
        private Long id;
        @JsonProperty("name")
        private String name;
        @JsonProperty("category")
        private String category;
        @JsonProperty("source")
        private String source;

    }

    private static final class TriggerWrapper extends RuleInfo.Trigger {

        @JsonIgnore
        @Override
        public void setResetInterval(Duration resetInterval) {
            super.setResetInterval(resetInterval);
        }

        @JsonIgnore
        @Override
        public void setDuration(Duration duration) {
            super.setDuration(duration);
        }

        @JsonProperty("interval")
        public void parserDuration(String duration) {
            setDuration(Duration.parse(duration));
        }

        @JsonProperty("resetInterval")
        public void parserResetInterval(String resetInterval) {
            setResetInterval(Duration.parse(resetInterval));
        }
    }

    private static final class RuleConfigJsonDeserializer
            extends JsonDeserializer<RuleInfo.RuleConfig> {

        @Override
        public RuleInfo.RuleConfig deserialize(
                final JsonParser jsonParser,
                final DeserializationContext deserializationContext) throws IOException {
            final ObjectCodec codec = jsonParser.getCodec();
            final JsonNode node = codec.readTree(jsonParser);

            if (node.has("knowledgeBase")) {
                return codec
                        .treeToValue(node, IndicatorConfigWrapper.class);
            }
            return codec
                    .treeToValue(node, RuleInfo.ThresholdConfig.class);
        }
    }
}
