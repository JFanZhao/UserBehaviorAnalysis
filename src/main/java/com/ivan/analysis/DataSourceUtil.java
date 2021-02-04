package com.ivan.analysis;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataSourceUtil {

    public static DataStreamSource<String> getData(StreamExecutionEnvironment env ){
        env.setParallelism(1);

        return env.readTextFile("/Users/ivan/dev/applications/git/UserBehaviorAnalysis/src/main/resources/UserBehavior.csv");
    }

}

