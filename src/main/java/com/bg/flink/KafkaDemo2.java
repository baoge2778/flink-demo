package com.bg.flink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch.util.RetryRejectedExecutionFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.http.HttpHost;
import org.apache.log4j.Logger;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.*;

/**
 * className:KafkaDemo
 * description:
 * author:
 * date:2019-07-08 14:05
 */
public class KafkaDemo2 {
    private static final Logger log = Logger.getLogger(KafkaDemo2.class.getSimpleName());


    public static void main(String[] args) throws Exception {
        System.out.println("===============》 flink任务开始  ==============》");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置kafka连接参数
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.16.0.140:9092"); //172.16.0.140
        properties.setProperty("zookeeper.connect", "172.16.0.140:2181");
        properties.setProperty("group.id", "con1");
        //设置时间类型
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置检查点时间间隔
        env.enableCheckpointing(5000);
        //创建kafak消费者，获取kafak中的数据
        FlinkKafkaConsumer010<String> kafkaConsumer010 = new FlinkKafkaConsumer010<String>("simData",new SimpleStringSchema(), properties);
        DataStreamSource<String> kafkaData = env.addSource(kafkaConsumer010);
        DataStream<Tuple3<Long, String, String>> mapData = kafkaData.map(new MapFunction<String, Tuple3<Long, String, String>>() {
            @Override
            public Tuple3<Long, String, String> map(String line) throws Exception {
                JSONObject jsonObject = JSON.parseObject(line);

                String dt = jsonObject.getString("dt");
                long time = 0;
                try {
//                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//                    Date parse = sdf.parse(dt);
//                    time = parse.getTime();
                    time = Long.parseLong(dt);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                String sim = jsonObject.getString("sim");
                String lonlat = jsonObject.getString("lonlat");
                return new Tuple3<>(time, sim, lonlat);
            }
        });

        List<HttpHost> esHttphost = new ArrayList<>();
        esHttphost.add(new HttpHost("172.16.0.140", 9200, "http"));
        ElasticsearchSink.Builder<Tuple3<Long, String, String>> esSinkBuilder = new ElasticsearchSink.Builder<Tuple3<Long, String, String>>(
                esHttphost,
                new ElasticsearchSinkFunction<Tuple3<Long, String, String>>() {
                    public IndexRequest createIndexRequest(Tuple3<Long, String, String> element) {
                        Map<String, Object> json = new HashMap<>();
                        json.put("time",element.f0);
                        json.put("sim",element.f1);
                        json.put("lonlat",element.f2);

                        //使用time+sim+lonlat 保证id唯一
//                        String id = element.f0 +"-"+element.f1;//+"-"+element.f2;

                        return Requests.indexRequest()
                                .index("simindex")
                                .type("simtype")
//                                .id(id)
                                .source(json);
                    }

                    @Override
                    public void process(Tuple3<Long, String, String> element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );


        esSinkBuilder.setBulkFlushMaxActions(100);
//        esSinkBuilder.setRestClientFactory(
//                restClientBuilder -> {
//                    restClientBuilder.setDefaultHeaders()
//                }
//        );
        esSinkBuilder.setRestClientFactory(new RestClientFactoryImpl());
        esSinkBuilder.setFailureHandler(new RetryRejectedExecutionFailureHandler());

        mapData.addSink(esSinkBuilder.build());
        env.execute("flink learning connectors kafka");
    }
}