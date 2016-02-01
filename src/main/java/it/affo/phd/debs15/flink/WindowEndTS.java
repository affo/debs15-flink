package it.affo.phd.debs15.flink;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.TimestampExtractor;

/**
 * Created by affo on 01/02/16.
 */
public class WindowEndTS<T extends Tuple> implements TimestampExtractor<T> {
    private long lastTS;

    @Override
    public long extractTimestamp(T element, long currentTimestamp) {
        long ts = element.getField(0);
        lastTS = Math.max(lastTS, ts);
        return ts;
    }

    @Override
    public long extractWatermark(T element, long currentTimestamp) {
        // let Flink decide when to emit watermarks
        return Long.MIN_VALUE;
    }

    @Override
    public long getCurrentWatermark() {
        long oneMinute = 1000 * 60;
        return lastTS - oneMinute;
    }
}
