package com.example.kafkaconsumeropensearch;

import com.google.gson.JsonParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

@Component
@Slf4j
public class OpenSearchConsumer {

    private final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getConsumerProperties());

    public OpenSearchConsumer() throws IOException {
        log.info("ConsumerDemoCooperative started");

        // get a reference to the main thread
        final Thread mainThread = Thread.currentThread();

        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detected shutdown, let's exit by calling wakeup()");
            consumer.wakeup(); // next time we poll(), a WakeUpException is thrown

            // join the main thread to allow the execution of the code in the main thread
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                log.info("");
            }
        }));

        // create OpenSearch client
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        // create index on OpenSearch if it doesn't exist already
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");

//        try (openSearchClient; consumer) {
//            if (!openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT)) {
//                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
//                log.info("Wikimedia Index created");
//            } else {
//                log.info("Wikimedia Index already exists");
//            }
//        } catch (IOException e) {
//            log.error("Error occurred with OpenSearch client");
//        }

        try {
            consumer.subscribe(List.of("wikimedia.recentchange"));
            while(true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));
                int recordCount = records.count();
                log.info("Records received: {}", recordCount);
                BulkRequest bulkRequest = new BulkRequest();

                for(ConsumerRecord<String, String> record : records) {
                    try {
                        String id = extractId(record.value());


                        IndexRequest indexRequest = new IndexRequest("wikimedia")
                                .source(record.value(), XContentType.JSON)
                                .id(id);
//                        IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
                        bulkRequest.add(indexRequest);
//                        log.info(response.getId());
                    } catch (Exception e) {}
                }
                if (bulkRequest.numberOfActions() > 0) {
                    BulkResponse bulkItemResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("Inserted {} record(s)", bulkItemResponse.getItems().length);
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    log.info("");
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer is starting to shut down");
        } catch (Exception e) {
            log.error("Unknown exception occured in consumer", e);
        } finally {
            consumer.close(); // closes the consumer and commits the offsets
            openSearchClient.close();
            log.info("Consumer has been gracefully shut down");
        }
    }

    private String extractId(String json) {
        return JsonParser.parseString(json).getAsJsonObject()
                .get("meta").getAsJsonObject()
                .get("id").getAsString();
    }

    public static RestHighLevelClient createOpenSearchClient() {
        String connectionString = "https://j89ldwu974:pybxwox3y1@kafka-test-5329777100.us-east-1.bonsaisearch.net:443";

        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connectionUri = URI.create(connectionString);
        String userInfo = connectionUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connectionUri.getHost(), connectionUri.getPort(), connectionUri.getScheme())));
        } else {
            String[] auth = userInfo.split(":");
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));
            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connectionUri.getHost(), connectionUri.getPort(), connectionUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));
        }
        return restHighLevelClient;
    }

    public Properties getConsumerProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "127.0.0.1:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "test");

        /**
         * 3 options: none, earliest, latest
         *      none - must set consumer group before starting application
         *      earliest - read from the beginning of the topic (--from-beginning)
         *      latest - read messages sent just now and after just now
         */
        props.put("auto.offset.reset", "earliest");

        props.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
        return props;
    }
}