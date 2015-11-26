package it.affo.phd.debs15.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.FileSinkFunctionByMillis;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class Main {
    private static final Logger LOG = LoggerFactory.getLogger("WordCount Example");
    private static final long WINDOW_GRANULARITY_IN_SECONDS = 60;
    private static final long PROFIT_WINDOW_IN_MINUTES = 15;
    private static final long EMPTY_TAXIS_WINDOW_IN_MINUTES = 30;
    private static final long PROFITABILITY_WINDOW_IN_MINUTES = 15;
    private static final long RATIO_INTERVAL_IN_SECONDS = 10;
    private static final int TOP_N = 10;
    public static final String INPUT_FILE_PATH = "file:///input_data.csv";
    public static final String OUTPUT_FILE_PATH = "file:///output.data";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // setting things up to enable EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().enableTimestamps();
        env.getConfig().setAutoWatermarkInterval(1000);

        DataStream<String> lines = env.readTextFile(INPUT_FILE_PATH);

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
        DataStream<Tuple2<TaxiRide, Double>> profit = rides
                .assignTimestamps(new DropoffTimestamp())
                .keyBy(
                        new KeySelector<TaxiRide, String>() {
                            @Override
                            public String getKey(TaxiRide value) throws Exception {
                                return value.dropoffCell;
                            }
                        })
                .timeWindow(
                        Time.of(PROFIT_WINDOW_IN_MINUTES, TimeUnit.MINUTES),
                        Time.of(WINDOW_GRANULARITY_IN_SECONDS, TimeUnit.SECONDS)
                )
                .apply(new ProfitFunction());


        // EMPTY TAXIS
        DataStream<Tuple2<TaxiRide, Integer>> emptyTaxis = rides
                .assignTimestamps(new DropoffTimestamp())
                .keyBy(
                        new KeySelector<TaxiRide, String>() {
                            @Override
                            public String getKey(TaxiRide value) throws Exception {
                                return value.taxiID;
                            }
                        })
                .timeWindow(
                        Time.of(EMPTY_TAXIS_WINDOW_IN_MINUTES, TimeUnit.MINUTES),
                        Time.of(WINDOW_GRANULARITY_IN_SECONDS, TimeUnit.SECONDS)
                )
                .apply(new EmptyTaxiFunction())
                .keyBy(
                        new KeySelector<Tuple2<TaxiRide, Integer>, String>() {
                            @Override
                            public String getKey(Tuple2<TaxiRide, Integer> value) throws Exception {
                                return value.f0.dropoffCell;
                            }
                        })
                .timeWindow(
                        Time.of(EMPTY_TAXIS_WINDOW_IN_MINUTES, TimeUnit.MINUTES),
                        Time.of(WINDOW_GRANULARITY_IN_SECONDS, TimeUnit.SECONDS)
                )
                .sum(1);


        // PROFITABILITY
        DataStream<Tuple2<TaxiRide, Double>> profitability = profit
                .join(emptyTaxis)
                .where(new ProfitWEmptyTaxisJoiner.JoinKey<Double>())
                .equalTo(new ProfitWEmptyTaxisJoiner.JoinKey<Integer>())
                .window(
                        TumblingTimeWindows.of(
                                Time.of(RATIO_INTERVAL_IN_SECONDS, TimeUnit.MINUTES)
                        )
                )
                .apply(new ProfitWEmptyTaxisJoiner(), profit.getType());


        // RANKING
        RankingSink rs = new RankingSink(TOP_N);
        rs.addOutput(new PrintSinkFunction<RankingSink.Ranking>());
        rs.addOutput(
                new FileSinkFunctionByMillis<>(
                        new TextOutputFormat<RankingSink.Ranking>(new Path(OUTPUT_FILE_PATH)), 0L)
        );
        profitability.global().addSink(rs).setParallelism(1);

        env.execute("DEBS 2015 - Profitability");
    }
}