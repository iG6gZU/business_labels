import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.HashMap;

public class ClickAndSearchTracking {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10 * 60 * 1000L);
        EmbeddedRocksDBStateBackend embeddedRocksDBStateBackend = new EmbeddedRocksDBStateBackend(true);
        embeddedRocksDBStateBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM);
        env.setStateBackend(embeddedRocksDBStateBackend);
        env.getCheckpointConfig().setCheckpointStorage("/user/flink/CheckPoint/App11/ClickAndSearchTracking");
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 业务名称字典
        HashMap<String, String> bizMap = new HashMap<String, String>() {{
            put("terminalModel", "terminal");
            put("broadbandModel", "broadband");
            put("packageModel", "package");
            put("cardModel", "number");
            put("familyModel", "family");
            put("xiaomiModel","xiaomi");
        }};

        //点击日志配置
        KafkaSourceBuilder<String> actionLogConsumer = KafkaSource.<String>builder()
                .setBootstrapServers("")
                .setTopics("")
                .setGroupId("")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("partition.discovery.interval.ms", "")
                .setProperty("security.protocol", "")
                .setProperty("sasl.mechanism", "")
                .setProperty("sasl.jaas.config", "");

        //接入点击日志
        SingleOutputStreamOperator<Tuple3<String, String, Long>> actionLogDS = env
                .fromSource(actionLogConsumer.build(), WatermarkStrategy.noWatermarks(), "App11.0点击日志接入")
                .flatMap((FlatMapFunction<String, Tuple3<String, String, Long>>) (s, collector) -> {
                    try {
                        JSONObject jsonObject = JSON.parseObject(s);
                        collector.collect(Tuple3.of(jsonObject.getString("uid"), bizMap.get(jsonObject.getString("biz_type")), jsonObject.getLong("ts")));
                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                    }
                })
                .returns(TypeInformation.of(new TypeHint<Tuple3<String, String, Long>>() {
                }))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner((SerializableTimestampAssigner<Tuple3<String, String, Long>>) (s, l) -> s.f2));


        //搜索日志配置
        KafkaSourceBuilder<String> searchLogConsumer = KafkaSource.<String>builder()
                .setBootstrapServers("")
                .setTopics("")
                .setGroupId("")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("partition.discovery.interval.ms", "")
                .setProperty("security.protocol", "")
                .setProperty("sasl.mechanism", "")
                .setProperty("sasl.jaas.config", "");

        //接入搜索日志
        SingleOutputStreamOperator<Tuple3<String, String, Long>> searchLogDS = env
                .fromSource(searchLogConsumer.build(), WatermarkStrategy.noWatermarks(), "App11.0搜索日志接入")
                .flatMap((FlatMapFunction<String, Tuple3<String, String, Long>>) (s, collector) -> {
                    try {
                        JSONObject jsonObject = JSON.parseObject(s);
                        collector.collect(Tuple3.of(jsonObject.getString("search_mob"), jsonObject.getString("biz_type"), jsonObject.getLong("ts")));
                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                    }
                })
                .returns(TypeInformation.of(new TypeHint<Tuple3<String, String, Long>>() {
                }))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner((SerializableTimestampAssigner<Tuple3<String, String, Long>>) (s, l) -> s.f2));


        actionLogDS
                .map((MapFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>>) val -> {
                    String s = DigestUtils.md5Hex(val.f0);
                    return Tuple3.of(s, val.f1, val.f2);
                })
                .returns(new TypeHint<Tuple3<String, String, Long>>() {
                })
                .addSink(new RichSinkFunction<Tuple3<String, String, Long>>() {
                    private Connection connection;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
                        conf.set("hbase.zookeeper.quorum", "");
                        connection = ConnectionFactory.createConnection(conf);
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                        connection.close();
                    }

                    @Override
                    public void invoke(Tuple3<String, String, Long> value, Context context) throws Exception {
                        super.invoke(value, context);

                        SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
                        String format1 = format.format(new Date(value.f2));

                        //todo 上线切换为生产表
                        Table table = connection.getTable(TableName.valueOf(Config.HBASE_TABLE_NAME));
                        Put put = new Put(Bytes.toBytes(value.f0));
                        put.setTTL(2 * 60 * 1000L);
                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("user_last_biz"), Bytes.toBytes("click" + "," + "code" + "," + value.f1 + "," + format1));
                        table.put(put);
                        table.close();
                    }
                });

        searchLogDS
                .map((MapFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>>) val -> {
                    String s = DigestUtils.md5Hex(val.f0);
                    return Tuple3.of(s, val.f1, val.f2);
                })
                .returns(new TypeHint<Tuple3<String, String, Long>>() {
                })
                .addSink(new RichSinkFunction<Tuple3<String, String, Long>>() {
                    private Connection connection;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
                        conf.set("hbase.zookeeper.quorum", "");
                        connection = ConnectionFactory.createConnection(conf);
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                        connection.close();
                    }

                    @Override
                    public void invoke(Tuple3<String, String, Long> value, Context context) throws Exception {
                        super.invoke(value, context);

                        SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
                        String format1 = format.format(new Date(value.f2));

                        //todo 上线切换为生产表
                        Table table = connection.getTable(TableName.valueOf(Config.HBASE_TABLE_NAME));
                        Put put = new Put(Bytes.toBytes(value.f0));
                        put.setTTL(2 * 60 * 1000L);
                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("user_last_biz"), Bytes.toBytes("search" + "," + "app" + "," + value.f1 + "," + format1));
                        table.put(put);
                        table.close();
                    }
                });

        env.execute("App11.0点击和搜索跟单");
    }
}
