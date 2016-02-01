package it.affo.phd.debs15.flink;

import org.apache.commons.math.stat.descriptive.rank.Median;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by affo on 25/11/15.
 */
public class ProfitFunction implements
        WindowFunction<
                TaxiRide,
                Tuple3<Long, String, Double>,
                String,
                TimeWindow> {

    @Override
    public void apply(
            String s,
            TimeWindow window,
            Iterable<TaxiRide> values,
            Collector<Tuple3<Long, String, Double>> out) throws Exception {
        List<Double> faretip = new ArrayList<>();
        for (TaxiRide tr : values) {
            faretip.add(tr.fare + tr.tip);
        }

        double[] gains = new double[faretip.size()];
        for (int i = 0; i < gains.length; i++) {
            gains[i] = faretip.get(i);
        }

        Arrays.sort(gains);

        double res = (new Median()).evaluate(gains);

        out.collect(new Tuple3<>(window.getEnd(), s, res));
    }
}
