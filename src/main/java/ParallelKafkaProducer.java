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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ParallelKafkaProducer {

    public static void main(String[] args) {
        String topicName = "test_topic_5"; // Replace with your actual topic name
        String bootstrapServers = "localhost:9092"; // Replace with your actual Kafka server address

        // Create the Kafka producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(props);

        // Create a thread pool
        ExecutorService executorService = Executors.newFixedThreadPool(10); // Pool size equal to the number of keys

        // Generate 10,000 events in parallel for different keys
        int numberOfEvents = 100;
        int numberOfKeys = 10;

        for (int key = 1; key <= numberOfKeys; key++) {
            int finalKey = key;
            executorService.submit(() -> {
                for (int i = 0; i < numberOfEvents / numberOfKeys; i++) {
                    int value = i + 1;
                    sendRecord(producer, topicName, String.valueOf(finalKey), String.valueOf(value));
                }
            });
        }

        // Shutdown the executor service
        executorService.shutdown();
        while (!executorService.isTerminated()) {
            // Wait for all tasks to complete
        }

        // Close the producer
        producer.close();
    }

    // Synchronized method to ensure sequential message sending per key
    private static synchronized void sendRecord(Producer<String, String> producer, String topic, String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                System.out.printf("Sent record with key=%s, value=%s to partition=%d, offset=%d%n",
                        key, value, metadata.partition(), metadata.offset());
            } else {
                exception.printStackTrace();
            }
        });
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
