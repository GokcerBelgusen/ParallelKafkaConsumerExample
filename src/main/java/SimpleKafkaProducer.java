import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;

import java.util.Map;
import java.util.Properties;

public class SimpleKafkaProducer {

    public static void main(String[] args) {
        String topicName = "test_topic_5";
        String bootstrapServers = "localhost:9092";

        // Create the Kafka producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(props);

        // Generate 10,000 events
        int numberOfEvents = 100;
        int numberOfKeys = 10;

        for (int i = 0; i < numberOfEvents; i++) {
            int key = (i % numberOfKeys) + 1;
            int value = (i / numberOfKeys) + 1;
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, String.valueOf(key), String.valueOf(value));
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.printf("Sent record with key=%s, value=%s to partition=%d, offset=%d%n",
                            key, value, metadata.partition(), metadata.offset());
                } else {
                    exception.printStackTrace();
                }
            });
        }

        // Close the producer
        producer.close();
    }

    public static class CustomPartitioner implements Partitioner {

        @Override
        public void configure(Map<String, ?> configs) {
            // No configuration needed for this example
        }

        @Override
        public int partition(String topic, Object key, byte[] keyBytes,
                             Object value, byte[] valueBytes, Cluster cluster) {
            int numberOfPartitions = cluster.partitionCountForTopic(topic);
            // Ensure partitions are distributed by key (1 to 10)
            return Utils.toPositive(Utils.murmur2(keyBytes)) % numberOfPartitions;
        }

        @Override
        public void close() {
            // No cleanup needed for this example
        }
    }
}
