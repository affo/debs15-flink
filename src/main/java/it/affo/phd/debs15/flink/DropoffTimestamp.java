package it.affo.phd.debs15.flink;

import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;

/**
 * Created by affo on 25/11/15.
 */
public class DropoffTimestamp extends AscendingTimestampExtractor<TaxiRide> {
    @Override
    public long extractAscendingTimestamp(TaxiRide element, long currentTimestamp) {
        return element.dropoffTS.getTime();
    }
}
