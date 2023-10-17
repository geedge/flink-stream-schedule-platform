package com.zdjizhi.schedule.indicator.functions;

import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.rule.SimpleRuleManager;
import com.zdjizhi.base.rule.cache.RuleCache;
import com.zdjizhi.base.rule.info.RuleInfo;
import com.zdjizhi.schedule.indicator.common.Configs;
import com.zdjizhi.schedule.indicator.rule.HttpRuleClient;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.http.client.methods.HttpGet;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Filtering record by rule id
 *
 * @author chaoc
 * @since 23.10
 */
public class RuleIDFilterFunction extends RichFilterFunction<CnRecordLog> {

    private transient RuleCache cache;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        cache = RuleCache.getInstance();

        final Configuration config = Configs.create();

        SimpleRuleManager<Long, RuleInfo> ruleManager =
                new SimpleRuleManager<>(new HttpRuleClient() {
                    @Override
                    protected HttpGet doGet(HttpGet httpGet) {
                        httpGet.addHeader("Accept", "application/json");
                        httpGet.addHeader("Cn-Authorization", config.get(Configs.RULE_API_TOKEN));
                        return httpGet;
                    }
                });

        ruleManager.startAsyncFlush(1000L, updatable -> {
            List<RuleInfo> list = updatable.getAddList();
            list.addAll(updatable.getUpdateList());

            Map<Long, RuleInfo> map = list.stream()
                    .collect(Collectors.toMap(RuleInfo::getRuleId, info -> info));
            cache.putAll(map);
            updatable.getDeleteKeys().forEach(cache::remove);
        });
    }

    @Override
    public boolean filter(CnRecordLog log) throws Exception {
        return log.getRule_id_list().stream()
                .anyMatch(id -> cache.get(id).isPresent()
                        && cache.get(id).get().isEnabled());
    }
}
