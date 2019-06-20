package com.microsoft.cse;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import javax.naming.ConfigurationException;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

/**
 * Hello world!
 *
 */
public class Consumer 
{
    private boolean running = true;
    private KafkaConsumer<byte[], BadgeEvent> consumer;
    private String clientId;
    private String groupId; // Example = "foo";
    private String bootstrapServers; // Example = "104.41.134.148:31090,40.121.16.99:31090,40.76.11.25:31090";
    private String topic; // Example = "test1";
    private String schemaRegistryUrl; // Example = 'http://localhost:8081';
    
    public Consumer()
    throws ConfigurationException, UnknownHostException
    {
        clientId = InetAddress.getLocalHost().getHostName();

        // Check Environment Config
        if (System.getenv("groupId") != null && !System.getenv("groupId").isEmpty())
        {
            groupId = System.getenv("groupId");
        }
        else
        {
            throw new ConfigurationException("ConsumerGroup 'groupId' has to be set example 'foo'");
        }

        if (System.getenv("bootstrapServers") != null && !System.getenv("bootstrapServers").isEmpty())
        {
            bootstrapServers = System.getenv("bootstrapServers");
        }
        else
        {
            throw new ConfigurationException("Kafka Endpoint 'bootstrapServers' has to be set example '104.41.134.148:31090', 'endpoint.mydomain.com:9092'");
        }

        if (System.getenv("topic") != null && !System.getenv("topic").isEmpty())
        {
            topic = System.getenv("topic");
        }
        else
        {
            throw new ConfigurationException("Kafka topic 'topic' has to be set example 'mytopic', 'events'");
        }

        if (System.getenv("schemaRegistryUrl") != null && !System.getenv("schemaRegistryUrl").isEmpty())
        {
            schemaRegistryUrl = System.getenv("schemaRegistryUrl");
        }
        else
        {
            throw new ConfigurationException("Kafka Schema Registry 'schemaRegistryUrl' has to be set example 'http://localhost:8081'");
        }


        if (System.getenv("clientId") != null && !System.getenv("clientId").isEmpty())
        {
            clientId = System.getenv("clientId");
        }
        else
        {
            System.out.printf("clientId not set using '%s' as clientId\n", clientId);
        }


        // Create Client
        Properties config = new Properties();
        config.put("client.id", clientId);
        config.put("group.id", groupId);
        config.put("bootstrap.servers", bootstrapServers);
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        config.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true); 

        consumer = new KafkaConsumer<byte[], BadgeEvent>(config);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(topic));

    }

    public void StopConsuming()
    {
        running = false;
    }

    public void StartConsuming()
    {
        while (running) {
            ConsumerRecords<byte[], BadgeEvent> records = consumer.poll(Duration.ofSeconds(10));
            process(records);
            try {
              consumer.commitSync();
            } catch (CommitFailedException e) {
              // application-specific rollback of processed records
            }
        }
        consumer.close();
    }

    private void process(ConsumerRecords<byte[], BadgeEvent> records)
    {
        records.forEach(record -> {
            System.out.printf("Consumer Record:(%s, %s, %d, %d)\n",
                record.key(), record.value(),
                record.partition(), record.offset());
        });
    }

}