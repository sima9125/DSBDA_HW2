package service;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaService
{
    private static final int STOPLOOP = 5;
    private static final String TOPIC = "stream-spark15";
    private Consumer<String, String> consumer;
    private String server = "localhost:9092";
    private Integer batchSize = 100;


    /**
     * Метод инициализации необходимых параметров
     */
    public void connect()
    {
        Map<String, Object> config = new HashMap<>();
        config.put("bootstrap.servers", server);
        config.put("client.id", "testclientid");
        config.put("group.id", "testgroupid");
        config.put("batch.size", batchSize);
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(config);
        List<TopicPartition> topicPartitionList = new ArrayList<>();
        topicPartitionList.add(new TopicPartition(TOPIC, 0));
        consumer.assign(topicPartitionList);
        consumer.seekToBeginning(topicPartitionList);
    }

    /**
     * чтение данных и синхронное отмечание факта прочитанности строки
     */
    private List<String> readData() {
        List<String> values = new ArrayList<>();
        consumer.poll(100).iterator().forEachRemaining(consumerRecord -> values.add(consumerRecord.value()));
        consumer.commitSync();
        return values;
    }

    /**
     * цикл, который читает данные несколько раз и завершается, если STOPLOOP раз ничего не было прочитано
     */
    public List<String> readAllData()
    {
        List<String> data = new ArrayList<>();

        for(int i=0; i<STOPLOOP; i++)
        {
            List<String> strings = readData();
            if(strings.size()>0)
            {
                i=0;
                data.addAll(strings);
            }

        }
        return data;
    }
}
