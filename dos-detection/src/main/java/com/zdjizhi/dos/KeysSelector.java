package com.zdjizhi.dos;

import com.zdjizhi.base.common.DosSketchLog;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

public class KeysSelector implements KeySelector<DosSketchLog, Tuple2<String, String>> {

    @Override
    public Tuple2<String, String> getKey(DosSketchLog dosSketchLog){
        return Tuple2.of(
                dosSketchLog.getAttack_type(),
                dosSketchLog.getDestination_ip());
    }

}
