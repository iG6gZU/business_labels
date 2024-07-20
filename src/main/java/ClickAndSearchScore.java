import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class ClickAndSearchScore {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10 * 60 * 1000L);
        EmbeddedRocksDBStateBackend embeddedRocksDBStateBackend = new EmbeddedRocksDBStateBackend(true);
        embeddedRocksDBStateBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM);
        env.setStateBackend(embeddedRocksDBStateBackend);
        env.getCheckpointConfig().setCheckpointStorage("/user/flink/CheckPoint/App11/ClickAndSearchScore");
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 业务顺序
        String[] bizType = new String[]{"terminal", "broadband", "package", "number", "family"};

        // 业务名称字典
        HashMap<String, String> bizMap = new HashMap<String, String>() {{
            put("terminalModel", "terminal");
            put("broadbandModel", "broadband");
            put("packageModel", "package");
            put("cardModel", "number");
            put("familyModel", "family");
        }};

        // 各业务点击加权指数
        HashMap<String, Long> clickExponent = new HashMap<String, Long>() {{
            put("terminal", 1L);
            put("broadband", 1L);
            put("package", 1L);
            put("number", 1L);
            put("family", 1L);
        }};

        //各业务搜索加权指数
        HashMap<String, Long> searchExponent = new HashMap<String, Long>() {{
            put("terminal", 1L);
            put("broadband", 1L);
            put("package", 1L);
            put("number", 1L);
            put("family", 1L);
        }};

        // 各业务类型权重
        HashMap<String, Long> bizWeight = new HashMap<String, Long>() {{
            put("terminal", 1L);
            put("broadband", 1L);
            put("package", 1L);
            put("number", 1L);
            put("family", 1L);
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


        actionLogDS.connect(searchLogDS).keyBy(data -> data.f0, data -> data.f0)
                .process(new KeyedCoProcessFunction<String, Tuple3<String, String, Long>, Tuple3<String, String, Long>, Tuple4<String, String, double[], long[]>>() {
                    private MapState<String, Long> bizClick1;
                    private MapState<String, Long> bizSearch1;
                    private MapState<String, Long> bizClick2;
                    private MapState<String, Long> bizSearch2;

                    private double[] mapValue(Long[] in) {
                        double sum = Arrays.stream(in).mapToDouble(Long::doubleValue).sum();
                        double avg = sum / in.length;
                        double std = Math.sqrt(Arrays.stream(in).mapToDouble(a -> (a - avg) * (a - avg)).sum() / in.length);
                        double[] norValue = Arrays.stream(in).mapToDouble(Long::doubleValue).map(a -> Math.pow(Math.E, (a - avg) / std)).toArray();
                        double sumValue = Arrays.stream(norValue).sum();
                        return Arrays.stream(norValue).map(a -> a / sumValue).toArray();
                    }

                    // 当前业务，今天的状态1，今天的状态2，昨天的状态1，昨天的状态2
                    private Tuple2<Long[], Long[]> checkState(String key, MapState<String, Long> todayBizState1, MapState<String, Long> todayBizState2, MapState<String, Long> yesterdayBizState1, MapState<String, Long> yesterdayBizState2, Map<String, Long> currentExp, Map<String, Long> anotherExp) throws Exception {
                        // 当前业务今天的状态（点击和搜索）
                        Long todayNum1 = todayBizState1.get(key);
                        Long todayNum2 = todayBizState2.get(key);

                        if (todayNum1 != null) {
                            todayNum1 += 1L;
                        } else {
                            todayNum1 = 1L;
                        }

                        if (todayNum2 == null) {
                            todayNum2 = 0L;
                        }

                        // 更新当前业务的状态
                        todayBizState1.put(key, todayNum1);
                        todayBizState2.put(key, todayNum2);

                        //取出当前业务今天的点击得分
                        //取出当前业务昨天的点击得分
                        //取出当前业务今天的搜索得分
                        //取出当前业务昨天的搜索得分
                        Long[] score = new Long[]{0L, 0L, 0L, 0L, 0L};
                        Long[] actions = new Long[]{0L, 0L, 0L, 0L, 0L};
                        for (int i = 0; i < bizType.length; i++) {
                            String k = bizType[i];
                            Long a1 = todayBizState1.get(k) != null ? todayBizState1.get(k) : 0L;
                            Long a2 = yesterdayBizState1.get(k) != null ? yesterdayBizState1.get(k) : 0L;
                            Long b1 = todayBizState2.get(k) != null ? todayBizState2.get(k) : 0L;
                            Long b2 = yesterdayBizState2.get(k) != null ? yesterdayBizState2.get(k) : 0L;
                            score[i] = ((a1 + a2) * currentExp.get(k) + (b1 + b2) * anotherExp.get(k)) * bizWeight.get(k);
                            actions[i] = a1+a2;
                        }

                        // 返回数组
                        return Tuple2.of(score, actions);
                    }

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        MapStateDescriptor<String, Long> bizClickDesc1 = new MapStateDescriptor<>("biz-click1", String.class, Long.class);
                        MapStateDescriptor<String, Long> bizSearchDesc1 = new MapStateDescriptor<>("biz-search1", String.class, Long.class);
                        MapStateDescriptor<String, Long> bizClickDesc2 = new MapStateDescriptor<>("biz-click2", String.class, Long.class);
                        MapStateDescriptor<String, Long> bizSearchDesc2 = new MapStateDescriptor<>("biz-search2", String.class, Long.class);

                        bizClick1 = getRuntimeContext().getMapState(bizClickDesc1);
                        bizSearch1 = getRuntimeContext().getMapState(bizSearchDesc1);
                        bizClick2 = getRuntimeContext().getMapState(bizClickDesc2);
                        bizSearch2 = getRuntimeContext().getMapState(bizSearchDesc2);
                    }

                    @Override
                    public void processElement1(Tuple3<String, String, Long> val, KeyedCoProcessFunction<String, Tuple3<String, String, Long>, Tuple3<String, String, Long>, Tuple4<String, String, double[], long[]>>.Context context, Collector<Tuple4<String, String, double[], long[]>> collector) throws Exception {
                        context.timerService().registerProcessingTimeTimer(System.currentTimeMillis() - (System.currentTimeMillis() + 8 * 3600000) % 86400000 + 86400000 + 1);
                        MapState<String, Long> todayClick;
                        MapState<String, Long> todaySearch;
                        MapState<String, Long> yesterdayClick;
                        MapState<String, Long> yesterdaySearch;

                        // 获取当前日期奇偶数，更新不同状态
                        long stateNum = System.currentTimeMillis() / (24 * 60 * 60 * 1000) % 2;
                        Tuple2<Long[], Long[]> scoreAndActions;
                        if (stateNum == 1) {
                            todayClick = bizClick1;
                            todaySearch = bizSearch1;
                            yesterdayClick = bizClick2;
                            yesterdaySearch = bizSearch2;
                        } else {
                            todayClick = bizClick2;
                            todaySearch = bizSearch2;
                            yesterdayClick = bizClick1;
                            yesterdaySearch = bizSearch1;
                        }

                        scoreAndActions = checkState(val.f1, todayClick, todaySearch, yesterdayClick, yesterdaySearch, clickExponent, searchExponent);
                        // 数值转换并输出
                        double[] resValue = mapValue(scoreAndActions.f0);
                        long[] array = Arrays.stream(scoreAndActions.f1).mapToLong(Long::longValue).toArray();
                        collector.collect(Tuple4.of(context.getCurrentKey(), "click", resValue, array));
                    }

                    @Override
                    public void processElement2(Tuple3<String, String, Long> val, KeyedCoProcessFunction<String, Tuple3<String, String, Long>, Tuple3<String, String, Long>, Tuple4<String, String, double[], long[]>>.Context context, Collector<Tuple4<String, String, double[], long[]>> collector) throws Exception {
                        context.timerService().registerProcessingTimeTimer(System.currentTimeMillis() - (System.currentTimeMillis() + 8 * 3600000) % 86400000 + 86400000 + 1);
                        MapState<String, Long> todayClick;
                        MapState<String, Long> todaySearch;
                        MapState<String, Long> yesterdayClick;
                        MapState<String, Long> yesterdaySearch;

                        // 获取当前日期奇偶数，更新不同状态
                        long stateNum = System.currentTimeMillis() / (24 * 60 * 60 * 1000) % 2;
                        Tuple2<Long[], Long[]> scoreAndActions;
                        if (stateNum == 1) {
                            todayClick = bizClick1;
                            todaySearch = bizSearch1;
                            yesterdayClick = bizClick2;
                            yesterdaySearch = bizSearch2;
                        } else {
                            todayClick = bizClick2;
                            todaySearch = bizSearch2;
                            yesterdayClick = bizClick1;
                            yesterdaySearch = bizSearch1;
                        }
                        scoreAndActions = checkState(val.f1, todaySearch, todayClick, yesterdaySearch, yesterdayClick, searchExponent, clickExponent);
                        // 数值转换并输出
                        double[] resValue = mapValue(scoreAndActions.f0);
                        long[] array = Arrays.stream(scoreAndActions.f1).mapToLong(Long::longValue).toArray();
                        collector.collect(Tuple4.of(context.getCurrentKey(), "search", resValue, array));
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedCoProcessFunction<String, Tuple3<String, String, Long>, Tuple3<String, String, Long>, Tuple4<String, String, double[], long[]>>.OnTimerContext ctx, Collector<Tuple4<String, String, double[], long[]>> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        long stateNum = System.currentTimeMillis() / (24 * 60 * 60 * 1000) % 2;
                        if (stateNum == 1) {
                            bizClick1.clear();
                            bizSearch1.clear();
                        } else {
                            bizClick2.clear();
                            bizSearch2.clear();
                        }
                    }
                })
                .map((MapFunction<Tuple4<String, String, double[], long[]>, Tuple4<String, String, double[], long[]>>) input -> {
                    String s = DigestUtils.md5Hex(input.f0);
                    return Tuple4.of(s, input.f1, input.f2, input.f3);
                })
                .returns(new TypeHint<Tuple4<String, String, double[], long[]>>() {
                })
                .addSink(new RichSinkFunction<Tuple4<String, String, double[], long[]>>() {
                    // ("mobile",得分)
                    private Connection connection;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
                        conf.set("hbase.zookeeper.quorum", "hbase01,hbase02,hbase03");
                        connection = ConnectionFactory.createConnection(conf);
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                        connection.close();
                    }

                    @Override
                    public void invoke(Tuple4<String, String, double[], long[]> value, Context context) throws Exception {
                        super.invoke(value, context);
                        Table table = connection.getTable(TableName.valueOf(Config.HBASE_TABLE_NAME));
                        Put put = new Put(Bytes.toBytes(value.f0));
                        put.setTTL(2 * 86400000L);
                        String val1 = String.format("%.5f,%.5f,%.5f,%.5f,%.5f", value.f2[0], value.f2[1], value.f2[2], value.f2[3], value.f2[4]);
                        String val2 = String.format("%d,%d,%d,%d,%d", value.f3[0], value.f3[1], value.f3[2], value.f3[3], value.f3[4]);

                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("biz_score"), Bytes.toBytes(val1));
                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("biz_"+value.f1), Bytes.toBytes(val2));

                        table.put(put);
                        table.close();
                    }
                });

        env.execute("App11.0点击和搜索得分");
    }
}
