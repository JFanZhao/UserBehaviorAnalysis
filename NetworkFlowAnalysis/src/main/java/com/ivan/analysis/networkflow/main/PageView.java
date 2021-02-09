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
package com.ivan.analysis.networkflow.main;

import com.ivan.analysis.FileUtil;
import com.ivan.analysis.bean.UserBehavior;
import lombok.Data;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.util.Random;

/**
 * @author ivan
 * @date 2021年02月09
 * pv
 */
public class PageView {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> inputStream = FileUtil.getData(env, FileUtil.DATA_PATH_BEHAVIOR);

        SingleOutputStreamOperator<UserBehavior> dataStream = inputStream.map(ub -> {
            String[] split = ub.split(",");
            return new UserBehavior(Long.valueOf(split[0]),
                    Long.valueOf(split[1]),
                    Long.valueOf(split[2]),
                    split[3],
                    Long.valueOf(split[4]));
        })//注册升序watermark
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBehavior>forMonotonousTimestamps().
                                withTimestampAssigner(((userBehavior, l) -> userBehavior.getTimestamp())));

        SingleOutputStreamOperator<PvCount> pvStream = dataStream.filter(u -> u.getBehavior().equals("pv"))
                .map(u -> {
                    return new Tuple2<String, Long>(Random.javaRandomToRandom(new java.util.Random()).nextString(10), 1L);
                }).keyBy((k) -> k.f0)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new PvCountAgg(), new PvCountWindowResult());
        SingleOutputStreamOperator<PvCount> result = pvStream.keyBy(p -> p.windowEnd)
                .process(new TotalPvCountResult());
        result.print();
        env.execute("pv");

    }

    @Data
    private static class PvCount {
        private Long windowEnd;
        private Long count;

        public PvCount(Long windowEnd, Long count) {
            this.windowEnd = windowEnd;
            this.count = count;
        }
    }

    private static class PvCountAgg implements AggregateFunction<Tuple2<String, Long>, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<String, Long> stringLongTuple2, Long aLong) {
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

    private static class PvCountWindowResult implements WindowFunction<Long, PvCount, String, TimeWindow> {
        @Override
        public void apply(String s, TimeWindow window, Iterable<Long> input, Collector<PvCount> out) throws Exception {
            out.collect(new PvCount(window.getEnd(), input.iterator().next()));
        }
    }

    private static class TotalPvCountResult extends KeyedProcessFunction<Long, PvCount, PvCount> {

        private ValueState<Long> totalPvCountResultState = null;

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<PvCount> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            Long totalCount = this.totalPvCountResultState.value();
            out.collect(new PvCount(ctx.getCurrentKey(), totalCount));
            totalPvCountResultState.clear();
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            this.totalPvCountResultState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("totcal-pv", Long.class));
        }

        @Override
        public void processElement(PvCount value, Context ctx, Collector<PvCount> out) throws Exception {

            Long curCount = this.totalPvCountResultState.value();
            totalPvCountResultState.update(curCount + value.getCount());
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);

        }
    }
}
