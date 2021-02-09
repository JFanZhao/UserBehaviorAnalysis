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
import com.ivan.analysis.networkflow.bean.ApacheLogEvent;
import com.ivan.analysis.util.DateUtils;
import lombok.Data;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

public class HotPagesNetworkFlow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //获取行为数据局
        DataStreamSource<String> inputStream = FileUtil.getData(env, FileUtil.DATA_PATH_APP_LOG);

        SingleOutputStreamOperator<ApacheLogEvent> dataStream = inputStream.map(data -> {
            String[] s = data.split(" ");
            long ts = DateUtils.parse(s[3]).getTime();
            return new ApacheLogEvent(s[0], s[1], ts, s[5], s[6]);
        })
                //注册watermark
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<ApacheLogEvent>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withTimestampAssigner((d, l) -> d.getTimestamp()));


        OutputTag<ApacheLogEvent> outputTag = new OutputTag("late", TypeInformation.of(ApacheLogEvent.class));

        SingleOutputStreamOperator<PageViewCount> aggStream = dataStream
                .filter(d -> d.getMethod().equals("GET"))
                .keyBy(ApacheLogEvent::getUrl)
                //窗口聚合，并写出指定格式 5秒滑动10分钟窗口
                .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.seconds(5)))
                //允许迟到数据
                .allowedLateness(Time.minutes(1))
                //侧输出流
                .sideOutputLateData(outputTag)
                .aggregate(new PageCountAgg(), new PageViewCountWindowResult());
        //聚合后，以窗口分组，对每个窗口内的数据处理
        SingleOutputStreamOperator<String> resultStream = aggStream
                .keyBy(PageViewCount::getWindowEnd)
                .process(new TopNHotPages(3));

        dataStream.print("data");
        aggStream.print("agg");
        aggStream.getSideOutput(outputTag).print("late");
        resultStream.print();

        env.execute("hot pages job");
    }

    @Data
    private static class PageViewCount {
        private String url;
        private Long windowEnd;
        private Long count;

        public PageViewCount(String url, Long windowEnd, Long count) {
            this.url = url;
            this.windowEnd = windowEnd;
            this.count = count;
        }
    }
//
//    @Data
//    private static class ApacheLogEvent {
//        private String ip;
//        private String userId;
//        private Long timestamp;
//        private String method;
//        private String url;
//
//        public ApacheLogEvent(String ip, String userId, Long timestamp, String method, String url) {
//            this.ip = ip;
//            this.userId = userId;
//            this.timestamp = timestamp;
//            this.method = method;
//            this.url = url;
//        }
//    }
    /**
     * 求和函数
     */

    private static class PageCountAgg implements AggregateFunction<ApacheLogEvent, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLogEvent apacheLogEvent, Long aLong) {
            return aLong + 1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return acc1 + aLong;
        }
    }

    /**
     * 窗口函数
     */
    private static class PageViewCountWindowResult implements WindowFunction<Long, PageViewCount, String, TimeWindow> {
        @Override
        public void apply(String url, TimeWindow window, Iterable<Long> input, Collector<PageViewCount> out) throws Exception {
            out.collect(new PageViewCount(url, window.getEnd(), input.iterator().next()));
        }
    }

    /**
     * process function
     * 分组取topN
     * 窗口延迟1毫秒触发，map状态存储每个url的数量
     * 另外还需要一个定时器，一分钟后触发，用来清除map的状态
     */
    private static class TopNHotPages extends KeyedProcessFunction<Long, PageViewCount, String> {

        private MapState<String, Long> pageViewCountMapState = null;
        private int n;

        public TopNHotPages(int n) {
            this.n = n;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            pageViewCountMapState = getRuntimeContext()
                    .getMapState(new MapStateDescriptor<String, Long>("pageViewCountMapState", String.class, Long.class));
        }

        @Override
        public void processElement(PageViewCount value, Context ctx, Collector<String> out) throws Exception {
            pageViewCountMapState.put(value.getUrl(), value.getCount());
            //注册1毫秒输出计时器
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
            //注册一分钟后清除状态计时器
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 60000);

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            //判断定时器触发时间，如果已经是窗口结束时间1分钟之后，那么直接清空状态
            if (timestamp == ctx.getCurrentKey() + 60000L) {
                pageViewCountMapState.clear();
                return ;
            }

            List<PageViewCount> allPageViewCounts = new ArrayList<>();
            Iterator<Map.Entry<String, Long>> iterator = pageViewCountMapState.entries().iterator();

            //取出数据
            while (iterator.hasNext()){
                Map.Entry<String, Long> next = iterator.next();
                allPageViewCounts.add(new PageViewCount(next.getKey(),timestamp,next.getValue()));
            }

            //排序
            List<PageViewCount> result = allPageViewCounts.stream()
                    .sorted(Comparator.comparing(PageViewCount::getCount).reversed())
                    .limit(n).collect(Collectors.toList());
            //输出
            StringBuffer sb = new StringBuffer();
            sb.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n");
            for (int i = 0; i < result.size(); i++) {
                PageViewCount currentItemViewCount = result.get(i);
                sb.append("NO").append(i + 1).append(": \t")
                        .append("页面URL = ").append(currentItemViewCount.getUrl()).append("\t")
                        .append("热门度 = ").append(currentItemViewCount.getCount()).append("\n");
            }
            sb.append("\n==================================\n\n");

            Thread.sleep(1000);
            out.collect(sb.toString());


        }


    }
}
