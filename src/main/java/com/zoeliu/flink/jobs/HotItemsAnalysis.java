package com.zoeliu.flink.jobs;

import com.zoeliu.flink.model.ItemViewCount;
import com.zoeliu.flink.model.UserBehavior;
import com.zoeliu.flink.operators.TopNWithOnTimer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class HotItemsAnalysis {

    public static void main(String[] args) throws Exception {
        // 1. Setup the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // Set parallelism to 1 for easier observation in console

        // 2. Read data from CSV file (Relative Path)
        DataStream<String> inputStream = env.readTextFile("src/main/resources/UserBehavior.csv");

        // 3. Map to POJO and Assign Watermarks
        DataStream<UserBehavior> dataStream = inputStream
                .map(line -> {
                    String[] arr = line.split(",");
                    return new UserBehavior(arr[0], arr[1], arr[2], arr[3], Long.parseLong(arr[4]) * 1000L);
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((element, recordTimestamp) -> element.timestamp));

        // 4. Core Logic: Filter -> KeyBy -> Window -> Aggregate
        // We want to find the top N hot items in the last 1 hour, updating every 5 minutes.
        DataStream<String> resultStream = dataStream
                .filter(data -> "pv".equals(data.behavior)) // Filter only 'pv' behavior
                .keyBy(data -> data.itemId) // Group by Item ID
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5))) // Sliding Window
                .aggregate(new ItemCountAgg(), new WindowResult()) // 1st Aggregation: Count per item
                .keyBy(data -> data.windowEnd) // Group by Window End Time (to sort items in the same window)
                .process(new TopNWithOnTimer(5)); // 2nd Aggregation: Sort and pick Top N

        // 5. Print result
        resultStream.print();

        env.execute("Hot Items Analysis Job");
    }

    /**
     * COUNT Aggregator: Calculate the count for each item.
     * Logic: Accumulator + 1 for every incoming element.
     */
    public static class ItemCountAgg implements AggregateFunction<UserBehavior, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    /**
     * WINDOW Result: Attach window information (end time) to the count result.
     * Output: ItemViewCount (itemId, windowEnd, count)
     */
    public static class WindowResult implements WindowFunction<Long, ItemViewCount, String, TimeWindow> {
        @Override
        public void apply(String itemId, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) {
            Long count = input.iterator().next();
            out.collect(new ItemViewCount(itemId, window.getEnd(), count));
        }
    }

}
