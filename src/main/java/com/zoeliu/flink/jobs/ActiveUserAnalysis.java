package com.zoeliu.flink.jobs;

import com.zoeliu.flink.model.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashSet;

public class ActiveUserAnalysis {

    public static void main(String[] args) throws Exception {
        // 1. Create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // easy to view result

        // 2. Read data
        // Make sure UserBehavior.csv is already in src/main/resources/
        DataStream<String> inputStream = env.readTextFile("src/main/resources/UserBehavior.csv");

        // 3. Convert to a POJO object and extract the timestamp.
        DataStream<UserBehavior> dataStream = inputStream
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] arr = value.split(",");
                        // The time in the CSV is in seconds, but Java/Flink requires milliseconds, so multiply by 1000.
                        return new UserBehavior(arr[0], arr[1], arr[2], arr[3], Long.parseLong(arr[4]) * 1000L);
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                            @Override
                            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        // 4. Grouping, Window Opening, Aggregation
        // We want to count the number of unique users (UV) per hour.
        dataStream.filter(data -> "pv".equals(data.behavior)) // Focus solely on click behavior
                .keyBy(data -> "active_user_stat") // All items are grouped together for statistical purposes.
                .window(TumblingEventTimeWindows.of(Time.hours(1))) // 1-hour rolling window
                .process(new ProcessWindowFunction<UserBehavior, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<UserBehavior> elements, Collector<String> out) throws Exception {
                        // use HashSet automatically remove duplicates
                        HashSet<String> activeUsers = new HashSet<>();
                        for (UserBehavior user : elements) {
                            activeUsers.add(user.userId);
                        }

                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        out.collect("Window: " + start + " ~ " + end + " | Active Users: " + activeUsers.size());
                    }
                })
                .print();

        // 5. Execute task
        env.execute("Active User Analysis Job");
    }
}