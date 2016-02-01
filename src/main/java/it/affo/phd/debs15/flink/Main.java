package it.affo.phd.debs15.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
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
    private static final int TOP_N = 10;
    public static final String INPUT_FILE_PATH = "file:///input_data.csv";
    public static final String OUTPUT_FILE_PATH = "file:///output.data";

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // setting things up to enable EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().enableTimestamps();

        DataStream<String> lines = env.readTextFile(INPUT_FILE_PATH).setParallelism(1);

        DataStream<TaxiRide> rides = lines.flatMap(new FlatMapFunction<String, TaxiRide>() {
            @Override
            public void flatMap(String value, Collector<TaxiRide> out) throws Exception {
                TaxiRide tr = TaxiRide.parseLine(value);
                if (tr != null) {
                    out.collect(tr);
                }
            }
        }).assignTimestamps(new AscendingTimestampExtractor<TaxiRide>() {
            @Override
            public long extractAscendingTimestamp(TaxiRide element, long currentTimestamp) {
                return element.dropoffTS.getTime();
            }
        });


        // PROFIT < ts, cell, profit >
        DataStream<Tuple3<Long, String, Double>> profit = rides
                .keyBy(
                        new KeySelector<TaxiRide, String>() {
                            @Override
                            public String getKey(TaxiRide value) throws Exception {
                                return value.pickupCell;
                            }
                        })
                .timeWindow(
                        Time.of(PROFIT_WINDOW_IN_MINUTES, TimeUnit.MINUTES),
                        Time.of(WINDOW_GRANULARITY_IN_SECONDS, TimeUnit.SECONDS)
                )
                .apply(new ProfitFunction())
                // I think I have to implement a TimestampExtractor and
                // not an ascending one, because, after a keyBy, I do
                // not have any guarantee ef the ordering
                .assignTimestamps(new WindowEndTS<Tuple3<Long, String, Double>>());

        // EMPTY TAXIS < ts, cell, count >
        DataStream<Tuple3<Long, String, Integer>> emptyTaxis = rides
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
                // < windowEnd, taxiID, cell >
                .assignTimestamps(new WindowEndTS<Tuple3<Long, String, String>>())
                .keyBy(new KeySelector<Tuple3<Long, String, String>, String>() {
                    @Override
                    public String getKey(Tuple3<Long, String, String> value) throws Exception {
                        return value.f2;
                    }
                })
                // tumbling window of window granularity
                // in order to keep the results of only one window at a time
                .timeWindow(Time.of(WINDOW_GRANULARITY_IN_SECONDS, TimeUnit.SECONDS))
                .apply(new EmptyTaxisCounter())
                .assignTimestamps(new WindowEndTS<Tuple3<Long, String, Integer>>());

        // PROFITABILITY
        DataStream<Tuple3<Long, String, Double>> profitability = profit
                .join(emptyTaxis)
                .where(new ProfitWEmptyTaxisJoiner.ProfitJoinKey())
                .equalTo(new ProfitWEmptyTaxisJoiner.EmptyTaxisJoinKey())
                // tumbling window of window granularity
                .window(TumblingTimeWindows.of(Time.of(WINDOW_GRANULARITY_IN_SECONDS, TimeUnit.SECONDS)))
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