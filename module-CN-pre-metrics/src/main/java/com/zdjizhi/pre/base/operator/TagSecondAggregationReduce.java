package com.zdjizhi.pre.base.operator;

import com.zdjizhi.pre.base.common.CnMetricLog;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Union;
import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/4/7 10:53
 */
public class TagSecondAggregationReduce implements ReduceFunction<CnMetricLog> {
    @Override
    public CnMetricLog reduce(CnMetricLog value1, CnMetricLog value2) throws Exception {
        if (value1.getTag_server_ip_union() != null) {
            Union union = value1.getTag_server_ip_union();
            if (value2.getTag_server_ip_sketch() != null) {
                union.union(value2.getTag_server_ip_sketch());
            }
            if (value2.getTag_server_ip_union() != null) {
                union.union(value2.getTag_server_ip_union().getResult());
            }
        } else {
            Union union = SetOperation.builder().buildUnion();
            union.union(value1.getTag_server_ip_sketch());
            if (value2.getTag_server_ip_sketch() != null) {
                union.union(value2.getTag_server_ip_sketch());
            }
            if (value2.getTag_server_ip_union() != null) {
                union.union(value2.getTag_server_ip_union().getResult());
            }
            value1.setTag_server_ip_union(union);
            value1.setTag_server_ip_sketch(null);
        }

        if (value1.getTag_domain_union() != null) {
            Union union = value1.getTag_domain_union();
            if (value2.getTag_domain_sketch() != null) {
                union.union(value2.getTag_domain_sketch());
            }
            if (value2.getTag_domain_union() != null) {
                union.union(value2.getTag_domain_union().getResult());
            }
        } else {
            Union union = SetOperation.builder().buildUnion();
            union.union(value1.getTag_domain_sketch());
            if (value2.getTag_domain_sketch() != null) {
                union.union(value2.getTag_domain_sketch());
            }
            if (value2.getTag_domain_union() != null) {
                union.union(value2.getTag_domain_union().getResult());
            }
            value1.setTag_domain_union(union);
            value1.setTag_domain_sketch(null);
        }
        return value1;
    }
}
