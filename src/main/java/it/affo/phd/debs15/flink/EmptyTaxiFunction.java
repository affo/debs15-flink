package it.affo.phd.debs15.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Created by affo on 25/11/15.
 */
public class EmptyTaxiFunction implements
        WindowFunction<
                TaxiRide,
                Tuple2<TaxiRide, Integer>,
                String,
                TimeWindow> {

    @Override
    public void apply(
            String s,
            TimeWindow window,
            Iterable<TaxiRide> values,
            Collector<Tuple2<TaxiRide, Integer>> out) throws Exception {
        TaxiRide lastone = null;
        for (TaxiRide tr : values) {
            lastone = tr;
        }

        out.collect(new Tuple2<>(lastone, 1));
    }
}
