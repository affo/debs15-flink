package it.affo.phd.debs15.flink;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by affo on 26/11/15.
 */
public class RankingSink extends RichSinkFunction<Tuple3<Long, String, Double>> {
    private Ranking ranking;
    private List<RichSinkFunction<Ranking>> outs;

    public RankingSink(int length) {
        this.outs = new ArrayList<>();
        this.ranking = new Ranking(length);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        for (RichSinkFunction<Ranking> sf : outs) {
            sf.setRuntimeContext(getRuntimeContext());
            sf.open(parameters);
        }
    }

    @Override
    public void invoke(Tuple3<Long, String, Double> value) throws Exception {
        if (ranking.add(value)) {
            for (SinkFunction<Ranking> sf : outs) {
                sf.invoke(ranking);
            }
        }
    }

    public void addOutput(RichSinkFunction<Ranking> out) {
        outs.add(out);
    }

    public void clearOutputs() {
        outs.clear();
    }


    public static class Ranking implements Serializable {
        private Tuple3[] ranking;

        public Ranking(int length) {
            this.ranking = new Tuple3[length];
        }

        public boolean add(Tuple3<Long, String, Double> record) {
            boolean changed = false;
            int i;

            for (i = 0; i < ranking.length && !changed; i++) {
                @SuppressWarnings("unchecked")
                Tuple3<Long, String, Double> r = ranking[i];

                if (r == null || record.f2 > r.f2) {
                    changed = true;
                    i--;
                } else if (
                        r.f1.equals(record.f1) &&
                                r.f2.equals(record.f2)) {
                    // the record is already in the ranking
                    // discard this computation
                    return false;
                }
            }

            if (changed) {
                // now i points where we want...
                // we can right-shift
                for (int j = ranking.length - 1; j > i; j--) {
                    ranking[j] = ranking[j - 1];
                }

                // finally, add...
                ranking[i] = record;
            }

            return changed;
        }

        @Override
        public String toString() {
            String tos = "{\n";

            for (int i = 0; i < ranking.length; i++) {
                @SuppressWarnings("unchecked")
                Tuple3<Long, String, Double> r = ranking[i];

                if (r == null) {
                    tos += "\tNULL";
                } else {
                    tos += "\t\'" + r.f1 + "\': " + r.f2 + "\t(" + r.f0 + ")";
                }

                if (i < ranking.length - 1) {
                    tos += ',';
                }
                tos += '\n';
            }

            tos += "}\n";

            return tos;
        }
    }
}
