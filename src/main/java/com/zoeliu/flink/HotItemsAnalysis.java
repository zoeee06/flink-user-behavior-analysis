package com.zoeliu.flink;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class HotItemsAnalysis {

    public static void main(String[] args) throws Exception {
        // 1. Setup the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // Set parallelism to 1 for easier observation in console

        // 2. Read data from CSV file (Relative Path)
        DataStream<String> inputStream = env.readTextFile("src/main/resources/UserBehavior.csv");

        // 3. Map to POJO and Assign Watermarks
        DataStream<UserBehavior> dataStream = inputStream
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] arr = value.split(",");
                        return new UserBehavior(arr[0], arr[1], arr[2], arr[3], Long.parseLong(arr[4]) * 1000L);
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                            @Override
                            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        // 4. Core Logic: Filter -> KeyBy -> Window -> Aggregate
        // We want to find the top N hot items in the last 1 hour, updating every 5 minutes.
        DataStream<String> resultStream = dataStream
                .filter(data -> "pv".equals(data.behavior)) // Filter only 'pv' behavior
                .keyBy(data -> data.itemId) // Group by Item ID
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5))) // Sliding Window
                .aggregate(new ItemCountAgg(), new WindowResult()) // 1st Aggregation: Count per item
                .keyBy(data -> data.windowEnd) // Group by Window End Time (to sort items in the same window)
                .process(new TopN(3)); // 2nd Aggregation: Sort and pick Top N

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

    /**
     * TOP N Process Function: Sort items within the same window and output the Top N.
     */
    public static class TopN extends KeyedProcessFunction<Long, ItemViewCount, String> {
        private final int topSize;
        private ListState<ItemViewCount> itemState; // State to store all ItemViewCounts in the current window

        public TopN(int topSize) {
            this.topSize = topSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // Initialize the ListState
            itemState = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("item-state", ItemViewCount.class)
            );
        }

        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
            // Add the current item view count to the state list
            itemState.add(value);
            // Register a timer to trigger exactly 1ms after the window ends
            ctx.timerService().registerEventTimeTimer(value.windowEnd + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 1. Fetch all items from state
            List<ItemViewCount> allItems = new ArrayList<>();
            for (ItemViewCount item : itemState.get()) {
                allItems.add(item);
            }

            // 2. Clear state to free up memory
            itemState.clear();

            // 3. Sort by count (Descending order)
            allItems.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return Long.compare(o2.count, o1.count); // o2 - o1 for descending
                }
            });

            // 4. Build the output string for Top N
            StringBuilder result = new StringBuilder();
            result.append("========================================\n");
            result.append("Window End: ").append(new Timestamp(timestamp - 1)).append("\n");

            // Loop to get top N
            for (int i = 0; i < Math.min(topSize, allItems.size()); i++) {
                ItemViewCount currentItem = allItems.get(i);
                result.append("No.").append(i + 1).append(":")
                        .append("  ItemID=").append(currentItem.itemId)
                        .append("  Views=").append(currentItem.count)
                        .append("\n");
            }
            result.append("========================================\n");

            // 5. Output
            out.collect(result.toString());
        }
    }
}
