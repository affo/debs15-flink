import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class Main {
    private static final Logger LOG = LoggerFactory.getLogger("WordCount Example");

    public static final class SentenceSource implements SourceFunction<String> {
        public final static String[] words = {
                "We first compute aggregations on time-based windows of the data",
                "We partition our stream into windows of 10 seconds and slide the window every 5 seconds",
                "The most interesting event in the stream is when the price of a stock is changing rapidly"
        };

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            Random r = new Random();

            for (int i = 0; i < 20; i++) {
                Thread.sleep(r.nextInt(2000));
                int index = r.nextInt(words.length);
                String sentence = words[index];
                ctx.collect(sentence);
            }
        }

        @Override
        public void cancel() {

        }
    }

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> sentences;

        try {
            sentences = env.socketTextStream("172.17.42.1", 9999);
        } catch (Exception e) {
            sentences = env.addSource(new SentenceSource());
        }

        DataStream<Tuple2<String, Integer>> dataStream = sentences
                .flatMap(new Splitter())
                .keyBy(0)
                .timeWindow(Time.of(5, TimeUnit.SECONDS))
                .sum(1);

        dataStream.print();

        LOG.info("Executing");

        env.execute("Windowed WordCount");
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word : sentence.split(" ")) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }

}