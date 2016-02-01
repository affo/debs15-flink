package it.affo.phd.debs15.flink;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Created by affo on 25/11/15.
 */
public class EmptyTaxiFunction implements
        WindowFunction<
                TaxiRide,
                Tuple3<Long, String, String>,
                String,
                TimeWindow> {

    @Override
    public void apply(
            String s,
            TimeWindow window,
            Iterable<TaxiRide> values,
            Collector<Tuple3<Long, String, String>> out) throws Exception {
        TaxiRide lastone = null;
        for (TaxiRide tr : values) {
            lastone = tr;
        }

        if (lastone != null) {
            out.collect(new Tuple3<>(window.getEnd(), s, lastone.dropoffCell));
        }
    }
}
