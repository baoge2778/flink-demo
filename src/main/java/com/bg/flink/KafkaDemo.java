package com.bg.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
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
public class KafkaDemo {
    private static final Logger log = Logger.getLogger(KafkaDemo.class.getSimpleName());


    public static void main(String[] args) throws Exception {
        System.out.println("===============》 flink任务开始  ==============》");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置kafka连接参数
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.16.0.140:9092"); //172.16.0.140
        properties.setProperty("zookeeper.connect", "172.16.0.140:2181");
        properties.setProperty("group.id", "test-group");
        //设置时间类型
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置检查点时间间隔
        env.enableCheckpointing(5000);
        //创建kafak消费者，获取kafak中的数据
        FlinkKafkaConsumer010<String> kafkaConsumer010 = new FlinkKafkaConsumer010<String>("topic001",new SimpleStringSchema(), properties);
        DataStreamSource<String> kafkaData = env.addSource(kafkaConsumer010);
        DataStream<String> userData = kafkaData.map(new MapFunction<String, String>() {

            @Override
            public String map(String s) {
                log.info(">>>>>>接收topic报文:"+s+"<<<<<");
                return s;
            }
        });

        List<HttpHost> esHttphost = new ArrayList<>();
        esHttphost.add(new HttpHost("172.16.0.140", 9200, "http"));

        ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<>(
                esHttphost,
                new ElasticsearchSinkFunction<String>() {

                    public IndexRequest createIndexRequest(String element) {
                        Map<String, String> json = new HashMap<>();
                        json.put("data", element);
                        log.info("data:" + element);

                        return Requests.indexRequest()
                                .index("flink_index")
                                .type("flink_type")
                                .source(json);
                    }

                    @Override
                    public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );

        esSinkBuilder.setBulkFlushMaxActions(1);
//        esSinkBuilder.setRestClientFactory(
//                restClientBuilder -> {
//                    restClientBuilder.setDefaultHeaders()
//                }
//        );
        esSinkBuilder.setRestClientFactory(new RestClientFactoryImpl());
        esSinkBuilder.setFailureHandler(new RetryRejectedExecutionFailureHandler());

        userData.addSink(esSinkBuilder.build());
        env.execute("flink learning connectors kafka");
    }
}