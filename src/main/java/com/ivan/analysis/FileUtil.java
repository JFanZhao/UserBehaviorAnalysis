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
package com.ivan.analysis;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FileUtil {

    public static final String DATA_PATH_BEHAVIOR = "/Users/ivan/dev/applications/git/UserBehaviorAnalysis/src/main/resources/UserBehavior.csv";
    public static final String DATA_PATH_APP_LOG = "/Users/ivan/dev/applications/git/UserBehaviorAnalysis/src/main/resources/apache.log";
    public static DataStreamSource<String> getData(StreamExecutionEnvironment env,String path) {
        env.setParallelism(1);

        return env.readTextFile(path);
    }

//    public static DataStream<UserBehavior> tramsformAndAssignWatermark(DataStreamSource<String> inputStream) {
//
//        return inputStream.map(ub -> {
//            String[] split = ub.split(",");
//            return new UserBehavior(Long.valueOf(split[0]),
//                    Long.valueOf(split[1]),
//                    Long.valueOf(split[2]),
//                    split[3],
//                    Long.valueOf(split[4]));
//        })//注册升序watermark
//                .assignTimestampsAndWatermarks(
//                        WatermarkStrategy
//                                .<UserBehavior>forMonotonousTimestamps()
//                                //抽取时间戳
//                                .withTimestampAssigner(((userBehavior, l) -> userBehavior.getTimestamp())));
//    }

}

