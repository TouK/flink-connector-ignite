package pl.touk.flink.ignite;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;

public class StreamTableEnvironmentUtil {

    public static StreamTableEnvironment create() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();

        return StreamTableEnvironmentImpl.create(env, settings);
    }

}
