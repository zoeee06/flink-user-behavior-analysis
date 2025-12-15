package com.zoeliu.flink.jobs;

import com.zoeliu.flink.model.ItemViewCount;
import com.zoeliu.flink.model.UserBehavior;
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
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

public class PVTopNAnalysis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        // 1. Read data
        DataStream<String> inputStream = environment.readTextFile("src/main/resources/UserBehavior.csv");

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
    }

    public static class TopNItem extends KeyedProcessFunction<Long, ItemViewCount, String> {

        private ListState<ItemViewCount> itemState;
        private Integer threshold;

        public void open(Configuration parameter) throws Exception {
            super.open(parameter);
            itemState = getRuntimeContext().getListState(new ListStateDescriptor<>("item", ItemViewCount.class));
        }
        public TopNItem(Integer threshold) {
            this.threshold = threshold;
        }

        @Override
        public void processElement(ItemViewCount value, KeyedProcessFunction<Long, ItemViewCount, String>.Context ctx, Collector<String> out) throws Exception {
            itemState.add(value);
            ctx.timerService().registerEventTimeTimer(value.windowEnd + 100L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            ArrayList<ItemViewCount> allItem = new ArrayList<>();
            for (ItemViewCount item : itemState.get()) {
                allItem.add(item);
            }
            itemState.clear();
            allItem.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return Long.compare(o2.count, o1.count);
                }
            });

            StringBuilder result = new StringBuilder();
            result.append("=========================\n");
            result.append("time: ").append(new Timestamp(timestamp)).append("\n");
            for (int i = 0; i < this.threshold; i++) {
                ItemViewCount itemViewCount = allItem.get(i);
                result.append("No. ").append(i+1).append(" : ").append("ItemId: ").append(itemViewCount.itemId)
                        .append(" Count: ").append(itemViewCount.count)
                        .append("\n");
            }
            result.append("=========================\n");
            Thread.sleep(1000L);
            out.collect(result.toString());
        }

    }

    public static class WindowResult extends ProcessWindowFunction<Long, ItemViewCount, String, TimeWindow> {
        @Override
        public void process(String s,
                            ProcessWindowFunction<Long, ItemViewCount, String, TimeWindow>.Context context,
                            Iterable<Long> elements, Collector<ItemViewCount> out) throws Exception {
            out.collect(new ItemViewCount(s, context.window().getEnd(), elements.iterator().next()));
        }
    }

    public static class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {

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
}

