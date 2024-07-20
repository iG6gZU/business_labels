import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.hbase.sink.HBaseMutationConverter;
import org.apache.flink.connector.hbase.sink.HBaseSinkFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.time.Duration;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
public class ExposeCompensation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10 * 60 * 1000L);
        EmbeddedRocksDBStateBackend embeddedRocksDBStateBackend = new EmbeddedRocksDBStateBackend(true);
        embeddedRocksDBStateBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM);
        env.setStateBackend(embeddedRocksDBStateBackend);
        env.getCheckpointConfig().setCheckpointStorage("/user/flink/CheckPoint/App11/ExposeCompensation");
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        org.apache.hadoop.conf.Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", "");

        HashMap<String, String> bizMap = new HashMap<String, String>() {{
            put("terminalModel", "terminal");
            put("broadbandModel", "broadband");
            put("packageModel", "package");
            put("cardModel", "number");
            put("familyModel", "family");
            put("xiaomiModel","xiaomi");
        }};

        //曝光日志配置
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

        //接入曝光日志
        SingleOutputStreamOperator<JSONObject> actionLogDS = env
                .fromSource(actionLogConsumer.build(), WatermarkStrategy.noWatermarks(), "App11.0曝光日志接入")
                .flatMap((FlatMapFunction<String, JSONObject>) (s, collector) -> {
                    try {
                        collector.collect(JSON.parseObject(s));
                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                    }
                })
                .returns(JSONObject.class)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner((SerializableTimestampAssigner<JSONObject>) (s, l) -> s.getLong("ts")));


        actionLogDS.filter((FilterFunction<JSONObject>) jsonObject -> "首页-专享".equals(jsonObject.getString("storey")))
                .map((MapFunction<JSONObject, Tuple2<String, String>>) jsonObject -> Tuple2.of(jsonObject.getString("uid"), bizMap.get(jsonObject.getString("biz_type"))))
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
                }))
                .keyBy(data -> data.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, String>, Tuple3<String, String,String>>() {
                    private ValueState<Queue<String>> exposeQueue;

                    private final String[] bizType = new String[]{"terminal", "broadband", "package", "number","family","xiaomi"};

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        ValueStateDescriptor<Queue<String>> exposeQueueDesc = new ValueStateDescriptor<>("expose-queue", TypeInformation.of(new TypeHint<Queue<String>>() {
                        }));
                        StateTtlConfig stateTTL = StateTtlConfig.newBuilder(Time.days(7))
                                .setTtlTimeCharacteristic(StateTtlConfig.TtlTimeCharacteristic.ProcessingTime)
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                .build();
                        exposeQueueDesc.enableTimeToLive(stateTTL);
                        exposeQueue = getRuntimeContext().getState(exposeQueueDesc);
                    }

                    @Override
                    public void processElement(Tuple2<String, String> value, KeyedProcessFunction<String, Tuple2<String, String>, Tuple3<String, String,String>>.Context context, Collector<Tuple3<String, String,String>> collector) throws Exception {
                        // 定义一个队列，用来存储近12次曝光的业务
                        Queue<String> value1 = exposeQueue.value();
                        if (value1 == null) {
                            value1 = new LinkedList<>();
                        }
                        value1.offer(value.f1);
//                        int size = value1.size();
//                        for (int i = 0; i < size; i++) {
//                            Tuple2<Long, String> peek = value1.peek();
//                            if (peek != null && peek.f0 < System.currentTimeMillis() - 100L) {
//                                value1.poll();
//                            } else {
//                                break;
//                            }
//                        }

                        // 检查队列长度，如果超过12个则压出
                        int newSize = value1.size();
                        if (newSize > 12) {
                            value1.poll();
                        }

                        // 更新状态
                        exposeQueue.update(value1);

                        // 将队列转换成列表，使用stream统计每个业务出现的次数
                        ArrayList<String> list = new ArrayList<>(value1);
                        Map<String, Long> collect = list.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

                        // 检查统计结果小于5种业务的话，补齐剩下的业务，并设置次数为0
                        if (collect.size() < 5) {
                            for (String i : bizType) {
                                if (!collect.containsKey(i)) {
                                    collect.put(i, 0L);
                                }
                            }
                        }

                        // 记录统计结果，准备用于输出
                        StringBuilder tmp= new StringBuilder();
                        for (String key:collect.keySet()){
                            tmp.append(key).append(":").append(collect.get(key)).append(",");
                        }
                        String expose_his=tmp.substring(0, tmp.length() - 1);

                        // 对统计结果排序
                        List<Map.Entry<String, Long>> collect1 = collect
                                .entrySet().stream().sorted(Map.Entry.comparingByValue())
                                .collect(Collectors.toList());

                        // 输出
                        collector.collect(Tuple3.of(context.getCurrentKey(), collect1.get(0).getKey()+","+collect1.get(1).getKey(),expose_his));
                    }
                })
                .setParallelism(15)
                .map((MapFunction<Tuple3<String, String,String>, Tuple3<String, String,String>>) input -> {
                    String s = DigestUtils.md5Hex(input.f0);
                    return Tuple3.of(s, input.f1,input.f2);
                })
                .setParallelism(15)
                .returns(new TypeHint<Tuple3<String, String,String>>() {
                })
                .addSink(new HBaseSinkFunction<>(Config.HBASE_TABLE_NAME, hbaseConf, new HBaseMutationConverter<Tuple3<String, String,String>>() {
                    @Override
                    public void open() {
                    }

                    @Override
                    public Mutation convertToMutation(Tuple3<String, String,String> value) {
                        Put put = new Put(Bytes.toBytes(value.f0));
                        put.setTTL(7 * 86400000L);
                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("expose_least_biz_7d"), Bytes.toBytes(value.f1));
                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("expose_biz_his"), Bytes.toBytes(value.f2));
                        return put;
                    }
                }, 1024 * 1024 * 5, 1000, 3));
//                .setParallelism(30);

        env.execute("App11.0曝光补偿");
    }
}
