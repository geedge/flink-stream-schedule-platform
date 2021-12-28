package com.zdjizhi.dos.sink;

import com.zdjizhi.base.platform.Schedule;
import com.zdjizhi.etl.DosDetectionEtl;

/**
 * @author 94976
 */
public class OutputStreamSink implements Schedule {

    @Override
    public void schedule() throws Exception {
        DosDetectionEtl.getSketchSource().print();
    }
}
