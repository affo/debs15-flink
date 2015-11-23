package it.affo.phd.debs15.flink;

import org.apache.commons.math.stat.descriptive.rank.Median;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Main {
    private static final Logger LOG = LoggerFactory.getLogger("WordCount Example");
    private static final long WINDOW_GRANULARITY_IN_SECONDS = 60;
    private static final long PROFIT_WINDOW_IN_MINUTES = 15;
    private static final long EMPTY_TAXIS_WINDOW_IN_MINUTES = 30;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // setting things up to enable EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().enableTimestamps();
        env.getConfig().setAutoWatermarkInterval(1000);

        DataStream<String> lines = env.readTextFile("file:///input_data.csv");

        DataStream<TaxiRide> rides = lines.flatMap(new FlatMapFunction<String, TaxiRide>() {
            @Override
            public void flatMap(String value, Collector<TaxiRide> out) throws Exception {
                TaxiRide tr = TaxiRide.parseLine(value);
                if (tr != null) {
                    out.collect(tr);
                }
            }
        });

        // PROFIT
        DataStream<Tuple2<TaxiRide, Double>> profit = rides.assignTimestamps(
                new AscendingTimestampExtractor<TaxiRide>() {
                    @Override
                    public long extractAscendingTimestamp(TaxiRide element, long currentTimestamp) {
                        return element.dropoffTS.getTime();
                    }
                })
                .keyBy(new KeySelector<TaxiRide, String>() {
                    @Override
                    public String getKey(TaxiRide value) throws Exception {
                        return value.dropoffCell;
                    }
                })
                .timeWindow(
                        Time.of(PROFIT_WINDOW_IN_MINUTES, TimeUnit.MINUTES),
                        Time.of(WINDOW_GRANULARITY_IN_SECONDS, TimeUnit.SECONDS)
                )
                .apply(
                        new WindowFunction<TaxiRide, Tuple2<TaxiRide, Double>, String, TimeWindow>() {
                            @Override
                            public void apply(
                                    String s,
                                    TimeWindow window,
                                    Iterable<TaxiRide> values,
                                    Collector<Tuple2<TaxiRide, Double>> out) throws Exception {
                                List<Double> faretip = new ArrayList<>();
                                TaxiRide trigger = null;
                                for (TaxiRide tr : values) {
                                    faretip.add(tr.fare + tr.tip);
                                    trigger = tr;
                                }

                                double[] gains = new double[faretip.size()];
                                for (int i = 0; i < gains.length; i++) {
                                    gains[i] = faretip.get(i);
                                }

                                Arrays.sort(gains);

                                double res = (new Median()).evaluate(gains);

                                out.collect(new Tuple2<>(trigger, res));
                            }
                        });

        env.execute("DEBS 2015 - Profitability");
    }
}