package com.zdjizhi.dos.sink;

import com.zdjizhi.base.etl.DosDetectionEtl;
import com.zdjizhi.base.platform.Schedule;

/**
 * @author 94976
 */
public class OutputStreamSink implements Schedule {

    @Override
    public void schedule() throws Exception {
        DosDetectionEtl.getSketchSource().print();
    }
}
