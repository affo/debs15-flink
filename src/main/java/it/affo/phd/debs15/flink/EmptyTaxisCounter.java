package it.affo.phd.debs15.flink;

import org.apache.flink.api.java.tuple.Tuple3;
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
                Tuple3<Long, String, String>,
                Tuple3<Long, String, Integer>,
                String, TimeWindow> {

    @Override
    public void apply(
            String s, TimeWindow window,
            Iterable<Tuple3<Long, String, String>> values,
            Collector<Tuple3<Long, String, Integer>> out) throws Exception {
        // removing duplicates
        Set<String> taxis = new HashSet<>();
        // checking for windowID correctness
        long lastWID = -1L;

        for (Tuple3<Long, String, String> t : values) {
            taxis.add(t.f1);

            if (lastWID != -1L && t.f0 != lastWID) {
                throw new RuntimeException("Wrong windowID in this window: " + t.f0 + " != " + lastWID);
            }

            lastWID = t.f0;
        }

        out.collect(new Tuple3<>(window.getEnd(), s, taxis.size()));
    }
}
