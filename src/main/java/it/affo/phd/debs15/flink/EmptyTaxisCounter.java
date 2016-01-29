package it.affo.phd.debs15.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by affo on 1/25/16.
 */
public class EmptyTaxisCounter implements
        WindowFunction<
                Tuple2<TaxiRide, Integer>,
                Tuple2<TaxiRide, Integer>,
                String, TimeWindow> {

    @Override
    public void apply(
            String s, TimeWindow window,
            Iterable<Tuple2<TaxiRide, Integer>> values,
            Collector<Tuple2<TaxiRide, Integer>> out) throws Exception {
        // removing duplicates
        Set<String> taxis = new HashSet<>();
        TaxiRide trigger = null;

        for (Tuple2<TaxiRide, Integer> t : values) {
            taxis.add(t.f0.taxiID);
            trigger = t.f0;
        }

        if (trigger != null) {
            out.collect(new Tuple2<>(trigger, taxis.size()));
        }
    }
}
