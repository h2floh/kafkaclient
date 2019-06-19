package com.microsoft.cse;

import java.net.InetAddress;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Hello world!
 *
 */
public class Consumer 
{
    private boolean running = true;
    private KafkaConsumer<byte[], byte[]> consumer;
    private String clientId;
    private String groupId = "foo";
    private String bootstrapServers = "104.41.134.148:31090,40.121.16.99:31090,40.76.11.25:31090";
    private String topic = "test1";
    public Consumer()
    {
        try 
        {
            clientId = InetAddress.getLocalHost().getHostName();

            // Check Environment Config
            if (System.getenv("groupId") != null && !System.getenv("groupId").isEmpty())
            {
                groupId = System.getenv("groupId");
            }
            if (System.getenv("bootstrapServers") != null && !System.getenv("bootstrapServers").isEmpty())
            {
                bootstrapServers = System.getenv("bootstrapServers");
            }
            if (System.getenv("topic") != null && !System.getenv("topic").isEmpty())
            {
                topic = System.getenv("topic");
            }
            if (System.getenv("clientId") != null && !System.getenv("clientId").isEmpty())
            {
                clientId = System.getenv("clientId");
            }
            // Create Client
            Properties config = new Properties();
            config.put("client.id", clientId);
            config.put("group.id", groupId);
            config.put("bootstrap.servers", bootstrapServers);
            config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            consumer = new KafkaConsumer<byte[], byte[]>(config);

            // Subscribe to the topic.
            consumer.subscribe(Collections.singletonList(topic));
        }
        catch (Exception e)
        {
            System.out.printf("Oops, something went wrong %s\n", e.getMessage());
            e.printStackTrace();
            
        }
    }

    public void StopConsuming()
    {
        running = false;
    }

    public void StartConsuming()
    {
        while (running) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Long.MAX_VALUE);
            process(records);
            try {
              consumer.commitSync();
            } catch (CommitFailedException e) {
              // application-specific rollback of processed records
            }
        }
        consumer.close();
    }

    private void process(ConsumerRecords<byte[], byte[]> records)
    {
        records.forEach(record -> {
            System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",
                record.key(), record.value(),
                record.partition(), record.offset());
        });
    }

}