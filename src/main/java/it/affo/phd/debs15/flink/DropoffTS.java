package it.affo.phd.debs15.flink;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.TimestampExtractor;

/**
 * Created by affo on 12/11/15.
 */
public abstract class DropoffTS<E> implements TimestampExtractor<E> {
    private long ts, wm;

    @Override
    public long extractTimestamp(E element, long currentTimestamp) {
        ts = getDropoffTS(element, currentTimestamp);
        return ts;
    }

    @Override
    public long extractWatermark(E element, long currentTimestamp) {
        wm = getDropoffTS(element, currentTimestamp) + 1000L;
        return wm;
    }

    @Override
    public long getCurrentWatermark() {
        return wm;
    }

    protected abstract long getDropoffTS(E element, long currentTimestamp);

    public static class forTaxiRide extends DropoffTS<TaxiRide> {
        @Override
        protected long getDropoffTS(TaxiRide element, long currentTimestamp) {
            return element.dropoffTS.getTime();
        }
    }

    public static class forTupleofTaxiRide<T extends Tuple> extends DropoffTS<T> {
        @Override
        protected long getDropoffTS(T element, long currentTimestamp) {
            return ((TaxiRide) element.getField(0)).dropoffTS.getTime();
        }
    }
}
