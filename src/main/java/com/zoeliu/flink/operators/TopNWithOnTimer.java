package com.zoeliu.flink.operators;

import com.zoeliu.flink.model.ItemViewCount;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class TopNWithOnTimer extends KeyedProcessFunction<Long, ItemViewCount, String> {

    private final int topSize;
    private ListState<ItemViewCount> itemState;

    public TopNWithOnTimer(int topSize) {
        this.topSize = topSize;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        itemState = getRuntimeContext().getListState(
                new ListStateDescriptor<>("item-state", ItemViewCount.class)
        );
    }

    @Override
    public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
        // 每来一条数据，先存入 State，暂不输出
        itemState.add(value);
        // 注册一个定时器，时间为 windowEnd + 1ms
        // 也就是等窗口内所有数据都到齐了（Watermark 过了 windowEnd），再触发 onTimer 排序
        ctx.timerService().registerEventTimeTimer(value.windowEnd + 1);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        // 1. 取出状态中的所有数据
        List<ItemViewCount> allItems = new ArrayList<>();
        for (ItemViewCount item : itemState.get()) {
            allItems.add(item);
        }

        // 2. 清空状态，释放内存
        itemState.clear();

        // 3. 排序 (倒序)
        allItems.sort((o1, o2) -> Long.compare(o2.count, o1.count));

        // 4. 拼接输出结果
        StringBuilder result = new StringBuilder();
        result.append("========================================\n");
        result.append("Window End: ").append(new Timestamp(timestamp - 1)).append("\n");

        for (int i = 0; i < Math.min(topSize, allItems.size()); i++) {
            ItemViewCount currentItem = allItems.get(i);
            result.append("No.").append(i + 1).append(":")
                    .append("  ItemID=").append(currentItem.itemId)
                    .append("  Views=").append(currentItem.count)
                    .append("\n");
        }
        result.append("========================================\n");

        // 5. 输出
        out.collect(result.toString());
    }
}
