package com.microsoft.cse;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import javax.naming.ConfigurationException;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

/**
 * Hello world!
 *
 */
public class Producer 
{
    private boolean running = true;
    private KafkaProducer<String, BadgeEvent> producer;
    private String clientId;
    private String groupId; // Example = "foo";
    private String bootstrapServers; // Example = "104.41.134.148:31090,40.121.16.99:31090,40.76.11.25:31090";
    private String topic; // Example = "test1";
    private String schemaRegistryUrl; // Example = 'http://localhost:8081';
    private String akcs; // Example = "all";
    private Integer retries; // Example = 0;

    public Producer()
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

        if (System.getenv("akcs") != null && !System.getenv("akcs").isEmpty())
        {
            akcs = System.getenv("akcs");
        }
        else
        {
            throw new ConfigurationException("Kafka producer akcs 'akcs' has to be set example 'all'");
        }
        
        if (System.getenv("retries") != null && !System.getenv("retries").isEmpty())
        {
            retries = Integer.parseInt(System.getenv("retries"));
        }
        else
        {
            throw new ConfigurationException("Kafka producer retries 'retries' has to be set example '1'");
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
        config.put(ProducerConfig.ACKS_CONFIG, akcs);
        config.put(ProducerConfig.RETRIES_CONFIG, retries);
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

        producer = new KafkaProducer<String, BadgeEvent>(config);
        StartProducing();

    }

    public void StopProducing()
    {
        running = false;
    }

    public void StartProducing()
    {
        while (running) {

            try
            {
                final String eventId = "id" + new Random().nextLong();
                final BadgeEvent event = new BadgeEvent();
                event.id = eventId;
                event.displayName = "name" + new Random().nextLong();
                event.userId = "userid" + new Random().nextLong();
                event.downVotes = new Random().nextInt();
                event.upVotes = new Random().nextInt();
                event.name = "name" + new Random().nextLong();
                event.reputation = "good";
                event.processedDate = new Date().toGMTString();
                
                final ProducerRecord<String, BadgeEvent> record = new ProducerRecord<String, BadgeEvent>(topic, event.getId().toString(), event);
                producer.send(record);
                producer.flush();
                System.out.printf("Successfully produced a messages to a topic called %s%n", topic);
                Thread.sleep(1000L);

            } catch (final SerializationException e) {
                e.printStackTrace();
            } catch (final InterruptedException e) {
                e.printStackTrace();
            }
        }
        producer.close();
    }

}