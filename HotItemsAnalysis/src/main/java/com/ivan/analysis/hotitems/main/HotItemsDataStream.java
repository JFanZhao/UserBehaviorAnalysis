/*
 * Copyright (C) 2021 The UserBehaviorAnalysis Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ivan.analysis.hotitems.main;

import com.ivan.analysis.bean.UserBehavior;
import com.ivan.analysis.FileUtil;
import lombok.Data;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author ivan
 * @date 2021年02月04日16:43:23
 * 热门商品浏览排行，data stream 版本
 */
public class HotItemsDataStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //***********flink 1.2 默认使用eventtime，不用单独设置
//        env.setStreamTimeCharacteristic();

        DataStreamSource<String> inputStream = FileUtil.getData(env);

        SingleOutputStreamOperator<UserBehavior> dataStream = inputStream.map(ub -> {
            String[] split = ub.split(",");
            return new UserBehavior(Long.valueOf(split[0]),
                    Long.valueOf(split[1]),
                    Long.valueOf(split[2]),
                    split[3],
                    Long.valueOf(split[4]));
        })//注册升序watermark
        .assignTimestampsAndWatermarks(
                WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                        //抽取时间戳
                .withTimestampAssigner(((userBehavior, l) -> userBehavior.getTimestamp())));
        SingleOutputStreamOperator<ItemViewCount> aggStream =
                dataStream
                        .filter(userBehavior -> userBehavior.getBehavior().equals("pv"))
                        .keyBy(UserBehavior::getItemId)
                        //滑动窗口，1小时，五分钟滑动一次
                        .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                        //聚合
                        .aggregate(new CountAggFunction(), new ItemViewWindowResult());
//        aggStream.print();
        //对aggstream的窗口进行分组，聚合，取topN
        SingleOutputStreamOperator<String> topStream = aggStream
                .keyBy(ItemViewCount::getWindowEnd)
                //按照窗口分组，收集当前窗口内的商品count数据
                .process(new TopNHotItems(5));

        topStream.print();
        env.execute("HotItemsDataStream");
    }


    /**
     * 自定义预聚合函数AggregateFunction，聚合状态就是当前商品的count值
     */
    private static class CountAggFunction implements AggregateFunction<UserBehavior, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior userBehavior, Long aLong) {
            return aLong + 1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return aLong + acc1;
        }
    }

    private static class ItemViewWindowResult implements WindowFunction<Long, ItemViewCount, Long, TimeWindow> {

        @Override
        public void apply(Long aLong, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
            Long key = aLong;
            Long windowEnd = window.getEnd();
            Long count = input.iterator().next();
            out.collect(new ItemViewCount(key, windowEnd, count));
        }
    }

    @Data
    private static class ItemViewCount {
        private Long itemId;
        private Long windowEnd;
        private Long count;

        public ItemViewCount(Long itemId, Long windowEnd, Long count) {
            this.itemId = itemId;
            this.windowEnd = windowEnd;
            this.count = count;
        }
    }

    /**
     * 按照窗口进行取topN，具体逻辑：</br>
     * 1、为每个key（窗口）建立一个state，存储所有的数据
     * 2、定义触发器，延迟一毫秒触发计算
     * 3、执行定时器任务时，对list排序去topN即可
     */
    private static class TopNHotItems extends KeyedProcessFunction<Long, ItemViewCount, String> {
        private int n = 5;

        //定义状态
        ListState<ItemViewCount> itemViewCountListState = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            //初始化状态
            itemViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("itemViewCount-list", ItemViewCount.class));
        }

        public TopNHotItems(int n) {
            this.n = n;
        }

        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {

            this.itemViewCountListState.add(value);
            //注册定时器，延迟1毫秒执行
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            List<ItemViewCount> allItems = new ArrayList<>();
            itemViewCountListState.get().forEach(item -> allItems.add(item));
            // 清空状态
            itemViewCountListState.clear();
            //排序取topN
            List<ItemViewCount> result = allItems.stream()
                    .sorted(Comparator.comparing(ItemViewCount::getCount).reversed())
                    .limit(n).collect(Collectors.toList());

            StringBuffer sb = new StringBuffer();
            sb.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n");
            for (int i = 0; i < result.size(); i++) {
                ItemViewCount currentItemViewCount = result.get(i);
                sb.append("NO").append(i + 1).append(": \t")
                        .append("商品ID = ").append(currentItemViewCount.getItemId()).append("\t")
                        .append("热门度 = ").append(currentItemViewCount.getCount()).append("\n");
            }
            sb.append("\n==================================\n\n");

            Thread.sleep(1000);
            out.collect(sb.toString());
        }


    }
}
