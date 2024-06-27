import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelStreamProcessor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class ParallelKafkaConsumerExample {

    public static void main(String[] args) {
        String inputTopic = "test_topic_5";
        String bootstrapServers = "localhost:9092";

        // Create Kafka consumer
        Consumer<String, String> kafkaConsumer = getKafkaConsumer(bootstrapServers);

        // Configure and build Parallel Consumer options
        var options = ParallelConsumerOptions.<String, String>builder()
                .ordering(ParallelConsumerOptions.ProcessingOrder.KEY) // Order by key
                .maxConcurrency(1000) // Set maximum concurrency
                .consumer(kafkaConsumer)
                .build();

        // Create the Parallel Stream Processor
        ParallelStreamProcessor<String, String> eosStreamProcessor =
                ParallelStreamProcessor.createEosStreamProcessor(options);

        // Subscribe to the input topic
        eosStreamProcessor.subscribe(Collections.singletonList(inputTopic));

        // Process events
        eosStreamProcessor.poll(record -> {
            // Your processing logic here
            String key = record.key();
            String value = record.value();
            System.out.printf("Processed record with key: %s, value: %s%n", key, value);

            // Optionally produce a new record based on the processed one
            // kafkaProducer.send(new ProducerRecord<>(outputTopic, key, processedValue));
        });

        // Add a shutdown hook to gracefully close the processor and producer
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            eosStreamProcessor.close();
            kafkaConsumer.close();
        }));
    }

    // Method to create Kafka consumer
    private static Consumer<String, String> getKafkaConsumer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "parallel-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Adjust as needed
        return new KafkaConsumer<>(props);
    }

}
