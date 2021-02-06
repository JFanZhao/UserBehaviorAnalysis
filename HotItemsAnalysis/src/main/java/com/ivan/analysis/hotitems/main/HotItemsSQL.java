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

import com.ivan.analysis.FileUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class HotItemsSQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStreamSource<String> inputStream = FileUtil.getData(env);

//        DataStream<UserBehavior> data = FileUtil.tramsformAndAssignWatermark(inputStream);

        //定义表执行环境
//        EnvironmentSettings settings = EnvironmentSettings
//                .newInstance()
//                .inStreamingMode()
//                .useBlinkPlanner()
//                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        //ddl 语句，从csv读取数据,
        // 这里有个注意的，一开始app_time(13位时间戳)定义为timestamp，死活不行。必须做转换才可以
        String ddl ="create table user_behavior (\n" +
                "\tuser_id bigint,\n" +
                "\titem_id bigint,\n" +
                "\tcategoryId bigint,\n" +
                "\tbehavior STRING,\n" +
                "\tapp_time bigint,\n" +
                "\tts AS TO_TIMESTAMP(FROM_UNIXTIME(app_time / 1000, 'yyyy-MM-dd HH:mm:ss')),\n" +
                "\tWATERMARK FOR ts AS ts - INTERVAL '3' SECOND\n" +
                ") WITH\n" +
                "( \n" +
                "\t'connector.type' = 'filesystem',\n" +
                "\t'connector.path' = '" + FileUtil.DATA_PATH_BEHAVIOR + "',\n" +
                "\t'format.type' = 'csv'\n" +
                ")\n";
        //执行ddl语句
        TableResult ddlResult = tableEnv.executeSql(ddl);


        //分组取topN语句
        String sql ="select *\n" +
                "      from (\n" +
                "          select\n" +
                "           *,\n" +
                "           row_number()\n" +
                "           over (partition by windowEnd order by cnt desc)\n" +
                "             as row_num\n" +
                "          from (\n" +
                "\t           select\n" +
                "\t             item_id,\n" +
                "\t             hop_end(ts, interval '5' minute, interval '1' hour) as windowEnd,\n" +
                "\t             count(item_id) as cnt\n" +
                "\t         from user_behavior\n" +
                "\t            where behavior = 'pv'\n" +
                "\t            group by\n" +
                "\t             item_id,\n" +
                "\t             hop(ts, interval '5' minute, interval '1' hour)\n" +
                "           )\n" +
                "        )\n" +
                " where row_num <= 5";


        Table table = tableEnv.sqlQuery(sql);
        tableEnv.toRetractStream(table, Row.class).print();
        env.execute("hot item sql");

    }
}
