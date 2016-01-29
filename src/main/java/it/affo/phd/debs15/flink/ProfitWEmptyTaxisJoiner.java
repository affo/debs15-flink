package it.affo.phd.debs15.flink;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Created by affo on 25/11/15.
 */
public class ProfitWEmptyTaxisJoiner implements
        FlatJoinFunction<
                Tuple2<TaxiRide, Double>,
                Tuple2<TaxiRide, Integer>,
                Tuple2<TaxiRide, Double>> {

    @Override
    public void join(
            Tuple2<TaxiRide, Double> first,
            Tuple2<TaxiRide, Integer> second,
            Collector<Tuple2<TaxiRide, Double>> out) throws Exception {
        if (second.f1 == 0) {
            return;
        }

        double profitability = first.f1 / second.f1;

        //TaxiRide trigger = first.f0.dropoffTS.getTime() > second.f0.dropoffTS.getTime() ?
        //      first.f0 : second.f0;

        // We decide that the trigger is always the second one.
        // This will impact the cell output in rankings
        TaxiRide trigger = second.f0;
        out.collect(new Tuple2<>(trigger, profitability));
    }

    public static class ProfitJoinKey implements KeySelector<Tuple2<TaxiRide, Double>, String> {
        @Override
        public String getKey(Tuple2<TaxiRide, Double> value) throws Exception {
            TaxiRide tr = value.f0;
            return tr.pickupCell;
        }
    }

    public static class EmptyTaxisJoinKey implements KeySelector<Tuple2<TaxiRide, Integer>, String> {
        @Override
        public String getKey(Tuple2<TaxiRide, Integer> value) throws Exception {
            TaxiRide tr = value.f0;
            return tr.dropoffCell;
        }
    }
}
