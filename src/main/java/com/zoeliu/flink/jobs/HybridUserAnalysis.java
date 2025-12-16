package com.zoeliu.flink.jobs;

import com.zoeliu.flink.model.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class HybridUserAnalysis {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. Read data
        DataStream<String> inputStream = env.readTextFile("src/main/resources/UserBehavior.csv");

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

        // 2. Core Logic for writing daily report
        SingleOutputStreamOperator<String> reportStream = dataStream
                .keyBy(data -> data.userId)
                .process(new DualPurposeProcess(20));

        // 3. Processing side output stream (Alert -> Console)
        // Retrieve the Tag defined in our ProcessFunction
        OutputTag<String> alertTag = new OutputTag<String>("alert-side-output"){};
        DataStream<String> alertStream = reportStream.getSideOutput(alertTag);

        alertStream.print("🚨 ALERT");

        // 4. Processing MainStream (Report -> CSV File)
        reportStream.writeAsText("output/daily_report.csv", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        env.execute("Hybrid Analysis Job");
    }

    public static class DualPurposeProcess extends KeyedProcessFunction<String, UserBehavior, String> {
        private final int alertThreshold;
        private ValueState<Integer> countState;
        private ValueState<Boolean> timerState;

        // Define side output stream label (must be static or defined externally)
        private final OutputTag<String> alertTag = new OutputTag<String>("alert-side-output"){};

        public DualPurposeProcess(int alertThreshold) {
            this.alertThreshold = alertThreshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<>("count-state", Integer.class));
            timerState = getRuntimeContext().getState(new ValueStateDescriptor<>("timer-state", Boolean.class));
        }

        @Override
        public void processElement(UserBehavior value, Context ctx, Collector<String> out) throws Exception {
            Integer currentCount = countState.value();
            if (currentCount == null) currentCount = 0;
            currentCount++;
            countState.update(currentCount);

            // Core logic for real-time alert
            if (currentCount == alertThreshold) {
                String alertMsg = String.format("[RISK DETECTED] Time:%d | User:%s | Action: Threshold Exceeded (%d)",
                        value.timestamp, value.userId, currentCount);
                ctx.output(alertTag, alertMsg);
            }

            // Timer logic
            if (timerState.value() == null || !timerState.value()) {
                long now = ctx.timestamp();
                long nextDay = (now / (24 * 60 * 60 * 1000) + 1) * (24 * 60 * 60 * 1000);
                ctx.timerService().registerEventTimeTimer(nextDay);
                timerState.update(true);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            Integer totalCount = countState.value();
            if (totalCount != null && totalCount > 0) {
                // Format of daily report csv: UserId, Count
                out.collect(ctx.getCurrentKey() + "," + totalCount);
            }
            countState.clear();
            timerState.clear();
        }
    }
}
