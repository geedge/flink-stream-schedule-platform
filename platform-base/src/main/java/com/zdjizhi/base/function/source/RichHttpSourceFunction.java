package com.zdjizhi.base.function.source;

import org.apache.flink.api.common.functions.AbstractRichFunction;

public abstract class RichHttpSourceFunction<OUT> extends AbstractRichFunction implements HttpSourceFunction<OUT> {
    private static final long serialVersionUID = 1L;

    public RichHttpSourceFunction() {
    }
}
