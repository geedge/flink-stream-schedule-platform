package com.zdjizhi.schedule.indicator.functions;

import cn.hutool.core.lang.Snowflake;
import cn.hutool.core.util.IdUtil;
import com.zdjizhi.base.common.CnRecordLog;
import com.zdjizhi.base.rule.cache.RuleCache;
import com.zdjizhi.base.rule.info.RuleInfo;
import com.zdjizhi.schedule.indicator.record.MatchGroup;
import com.zdjizhi.schedule.indicator.record.SecurityEvent;
import com.zdjizhi.schedule.indicator.record.SlideAggregate;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Match Process
 *
 * @author chaoc
 * @since 23.10
 */
public class MatchKeyedProcessFunction
        extends KeyedProcessFunction<MatchGroup, SlideAggregate<CnRecordLog>, SecurityEvent> {

    private final static long TIMER_INTERVAL = 30 * 1000L;

    private transient RuleCache cache;

    private transient ValueState<LimitedSizeList<SlideAggregate<CnRecordLog>>> slideAggregateState;

    private transient ValueState<SecurityEvent> eventState;

    private transient Snowflake snowflake;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        snowflake = IdUtil.getSnowflake(
                Math.abs(getRuntimeContext().getJobId().hashCode() % (1 << 5)),
                Math.abs(getRuntimeContext().getTaskName().hashCode() % (1 << 5)));

        cache = RuleCache.getInstance();

        final ValueStateDescriptor<LimitedSizeList<SlideAggregate<CnRecordLog>>> listStateDescriptor =
                new ValueStateDescriptor<>("slide-aggregate-state",
                        TypeInformation.of(
                                new TypeHint<LimitedSizeList<SlideAggregate<CnRecordLog>>>() {}
                        )
                );
        slideAggregateState = getRuntimeContext().getState(listStateDescriptor);

        final ValueStateDescriptor<SecurityEvent> valueStateDescriptor =
                new ValueStateDescriptor<>("event-state", SecurityEvent.class);
        eventState = getRuntimeContext().getState(valueStateDescriptor);
    }

    @Override
    public void processElement(
            final SlideAggregate<CnRecordLog> slideValue,
            final KeyedProcessFunction<MatchGroup, SlideAggregate<CnRecordLog>, SecurityEvent>.Context ctx,
            final Collector<SecurityEvent> out) throws Exception {

        final TimerService timerService = ctx.timerService();
        final long current = timerService.currentWatermark();
        final long interval = TIMER_INTERVAL - (current % TIMER_INTERVAL);
        timerService.registerEventTimeTimer(current + interval);

        final long ruleId = ctx.getCurrentKey().getRuleId();
        Optional<RuleInfo> optional = cache.get(ruleId);

        if (!optional.isPresent()) {
            return;
        }

        final RuleInfo ruleInfo = optional.get();
        final LimitedSizeList<SlideAggregate<CnRecordLog>> stateValue =
                slideAggregateState.value();
        if (stateValue != null) {
            stateValue.add(slideValue);
        } else {
            final long minutes = ruleInfo.getTrigger().getDuration().toMinutes();
            final LimitedSizeList<SlideAggregate<CnRecordLog>> list =
                    new LimitedSizeList<>((int) minutes);
            list.add(slideValue);
            slideAggregateState.update(list);
        }
    }

    @Override
    public void onTimer(
            final long timestamp,
            final KeyedProcessFunction<MatchGroup, SlideAggregate<CnRecordLog>, SecurityEvent>.OnTimerContext ctx,
            final Collector<SecurityEvent> out) throws Exception {

        final long ruleId = ctx.getCurrentKey().getRuleId();
        final Optional<RuleInfo> optional = cache.get(ruleId);

        if (!optional.isPresent()) {
            return;
        }

        final RuleInfo ruleInfo = optional.get();
        final RuleInfo.Trigger trigger = ruleInfo.getTrigger();

        final SecurityEvent event = eventState.value();
        final LimitedSizeList<SlideAggregate<CnRecordLog>> logs = slideAggregateState.value();

        final long currentTimestamp = ctx.timerService().currentWatermark();

        if (event == null) {
            final Optional<SecurityEvent> securityEventOption =
                    this.generateOnGoingSecurityEventIfNeed(logs, ruleInfo,
                            ctx.getCurrentKey(), trigger.getDuration(),
                            trigger.getAtLeast(), currentTimestamp);

            if (!securityEventOption.isPresent()) {
                return;
            }

            final SecurityEvent e = securityEventOption.get();
            out.collect(e);

            slideAggregateState.clear();

        } else {
            final Optional<SecurityEvent> securityEventOption =
                    this.generateSecurityEventIfNeed(logs, event,
                            trigger.getResetInterval(), currentTimestamp);

            if (!securityEventOption.isPresent()) {
                return;
            }

            final SecurityEvent e = securityEventOption.get();
            out.collect(e);

            if (SecurityEvent.EventStatus.ON_GOING.equals(e.getStatus())) {
                eventState.update(e);
                slideAggregateState.clear();
            } else {
                eventState.clear();
            }
        }
    }

    Optional<SecurityEvent> generateOnGoingSecurityEventIfNeed(
            final LimitedSizeList<SlideAggregate<CnRecordLog>> logs,
            final RuleInfo info, final MatchGroup group,
            final Duration matchDuration,
            final int matchTimes, final long currentTime) {
        if (logs == null) {
            return Optional.empty();
        }
        // match_duration
        if (logs.size() < matchDuration.toMinutes()) {
            return Optional.empty();
        }
        // match_times
        final int totalTimes = logs.stream()
                .mapToInt(SlideAggregate::getTimes).sum();
        if (totalTimes < matchTimes) {
            return Optional.empty();
        }

        final SecurityEvent event = new SecurityEvent();
        event.setEventId(snowflake.nextId());
        event.setEventType(info.getEventType());
        event.setEventName(info.getName());
        event.setEventKey(group.toString());

        event.setRuleId(info.getRuleId());
        event.setRuleType(info.getRuleType());

        event.setBuiltin(info.isBuiltIn());
        event.setMatchTimes(matchTimes);

        final SecurityEvent.EventInfo eventInfo = new SecurityEvent.EventInfo();

        if (info.getRuleConfig() instanceof RuleInfo.IndicatorMatchConfig) {
            final RuleInfo.IndicatorMatchConfig ruleConfig =
                    ((RuleInfo.IndicatorMatchConfig) info.getRuleConfig());
            final RuleInfo.Level level = ruleConfig.getLevel();
            event.setSeverity(level);

            eventInfo.setLibraryId(ruleConfig.getLibraryId());
            eventInfo.setName(ruleConfig.getLibraryName());
        }

        event.setOffenderIp(group.getClientIp());
        event.setVictimIp(group.getServerIp());

        final SlideAggregate<CnRecordLog> first = logs.getFirst();

        long firstTime = first.getFirstTime();
        event.setStartTime((int) (firstTime / 1000L));

        // Use the last element's time
        // final SlideAggregate<CnRecordLog> last = logs.getLast();
        // long lastTime = last.getLastTime();
        // event.setDurationS((int) (lastTime / 1000L) - event.getStartTime());

        // Use system time
        int endTime = (int) (currentTime / 1000L);
        event.setDurationS(endTime - event.getStartTime());
        event.setStatus(SecurityEvent.EventStatus.ON_GOING);
        event.setEndTime(endTime);

        final CnRecordLog log = first.getValue();
        if (!info.isBuiltIn()) {
            if (log.getDomain_tags() != null && log.getDomain_tags().size() > 0) {
                eventInfo.setIocType("DOMAIN");
                eventInfo.setIocValue(log.getDomain_tags().get(0));
            } else if (log.getClient_ip_tags() != null && log.getClient_ip_tags().size() > 0) {
                eventInfo.setIocType("IP");
                eventInfo.setIocValue(log.getClient_ip_tags().get(0));
            } else if (log.getServer_ip_tags() != null && log.getServer_ip_tags().size() > 0) {
                eventInfo.setIocType("IP");
                eventInfo.setIocValue(log.getServer_ip_tags().get(0));
            } else if (log.getApp_tags() != null && log.getApp_tags().size() > 0) {
                eventInfo.setIocType("APP");
                eventInfo.setIocValue(log.getApp_tags().get(0));
            }
        } else {
            if (log.getIoc_type_list() != null && log.getIoc_type_list().size() > 0) {
                final String type = log.getIoc_type_list().get(0);
                eventInfo.setIocType(type);
                switch (type.toLowerCase()) {
                    case "ip":
                        eventInfo.setIocValue(log.getCommon_server_ip());
                        break;
                    case "domain":
                        eventInfo.setIocValue(log.getDomain());
                        break;
                    case "url":
                        eventInfo.setIocValue(log.getHttp_url());
                        break;
                    case "app":
                        eventInfo.setIocValue(log.getCommon_app_label());
                        break;
                }
            }
        }

        event.setEventInfo(eventInfo.toString());
        return Optional.of(event);
    }

    Optional<SecurityEvent> generateSecurityEventIfNeed(
            final LimitedSizeList<SlideAggregate<CnRecordLog>> logs,
            final SecurityEvent onGoingEvent,
            final Duration resetInterval, final long currentTime) {

        if (onGoingEvent == null) {
            return Optional.empty();
        }

        final int lastTime = onGoingEvent.getStartTime() + onGoingEvent.getDurationS();

        final List<SlideAggregate<CnRecordLog>> incLogs = logs.stream()
                .filter(slide -> slide.getLastTime() / 1000L > lastTime)
                .collect(Collectors.toList());

        final int endTime = (int) (currentTime / 1000L);
        if (incLogs.size() > 0) {
            final int incTimes = sum(incLogs);
            onGoingEvent.setMatchTimes(onGoingEvent.getMatchTimes() + incTimes);
            onGoingEvent.setDurationS(endTime - onGoingEvent.getStartTime());
            onGoingEvent.setEndTime(endTime);
            return Optional.of(onGoingEvent);
        } else if (lastTime + resetInterval.toMinutes() * 60 <= currentTime / 1000L) {
            onGoingEvent.setStatus(SecurityEvent.EventStatus.ENDED);
            return Optional.of(onGoingEvent);
        } else {
            return Optional.of(onGoingEvent);
        }
    }

    // ======================================================================================
    // ----------------------------------- private helper -----------------------------------

    private int sum(List<SlideAggregate<CnRecordLog>> slideValues) {
        return slideValues.stream().mapToInt(SlideAggregate::getTimes).sum();
    }

    private static class LimitedSizeList<E> extends LinkedList<E> {

        private final int maxSize;

        public LimitedSizeList(final int maxSize) {
            this.maxSize = maxSize;
        }

        @Override
        public boolean add(E e) {
            super.add(e);
            if (size() > maxSize) {
                super.removeFirst();
            }
            return true;
        }
    }

}
