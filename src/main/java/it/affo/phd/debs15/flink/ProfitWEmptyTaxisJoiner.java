package it.affo.phd.debs15.flink;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

/**
 * Created by affo on 25/11/15.
 */
public class ProfitWEmptyTaxisJoiner implements
        FlatJoinFunction<
                Tuple3<Long, String, Double>,
                Tuple3<Long, String, Integer>,
                Tuple3<Long, String, Double>> {

    @Override
    public void join(
            Tuple3<Long, String, Double> first,
            Tuple3<Long, String, Integer> second,
            Collector<Tuple3<Long, String, Double>> out) throws Exception {
        if (!first.f0.equals(second.f0)) {
            throw new RuntimeException("Mismatching window IDs: " + first.f0 + " != " + second.f0);
        }

        if (second.f2 == 0) {
            return;
        }

        double profitability = first.f2 / second.f2;

        out.collect(new Tuple3<>(first.f0, first.f1, profitability));
    }

    public static class ProfitJoinKey implements KeySelector<Tuple3<Long, String, Double>, String> {
        @Override
        public String getKey(Tuple3<Long, String, Double> value) throws Exception {
            return value.f1;
        }
    }

    public static class EmptyTaxisJoinKey implements KeySelector<Tuple3<Long, String, Integer>, String> {
        @Override
        public String getKey(Tuple3<Long, String, Integer> value) throws Exception {
            return value.f1;
        }
    }
}
