package com.example.listener;

import com.example.dto.MachineEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.streams.KafkaStreamsInteractiveQueryService;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
@Configuration
public class MachineEventStreamProcessorConfig {




    private final ObjectMapper mapper = new ObjectMapper();

    @Bean
    public KStream<String, String> machineStream(StreamsBuilder builder) {

        String storeName = "machine-status-store";

        // 1. Define a persistent key-value store (backed by RocksDB)
        StoreBuilder<KeyValueStore<String, String>> storeBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(storeName),   // persistent store backed by RocksDB
//                    Stores.persistentKeyValueStore(storeName) =>    This method is specifically for creating a persistent store, and Kafka Streams uses RocksDB under the hood by default for this type of persistent store.
//                     Stores.inMemoryKeyValueStore("my-store")	In-Memory	Heap-based (Java Map-like, no disk persistence)
                        Serdes.String(), // Key serializer (String)
                        Serdes.String() // Value serializer (String)
                );
        builder.addStateStore(storeBuilder);  // Add the store to the builder
/*
What Happens Here?
Persistent Key-Value Store:

You're creating a persistent key-value store called machine-status-store, which is backed by RocksDB.

This store is persistent, meaning data written to it will survive restarts of the Kafka Streams application. It also ensures durability — if the application crashes, data in the store won’t be lost.

RocksDB is the underlying storage mechanism that handles the write and read operations on this store.

Key and Value Serdes (Serializer/Deserializer):

You're using Serdes.String() for both the key and the value. This means that you're storing String keys and String values in the store. Specifically, in your case, you're storing the machineId as the key and the status as the value.

Kafka Streams uses Serdes (short for serializers and deserializers) to convert between the object representations and byte arrays that are sent over the Kafka bus or stored in the local state.

Store Addition:

The builder.addStateStore(storeBuilder) adds the RocksDB-backed store to your Kafka Streams topology.

This makes the store available for use in your stream processing logic.


 */



        KStream<String, String> stream = builder.stream("mytopic");

        // 2. Parse JSON into MachineEvent
        KStream<String, MachineEvent> parsedStream = stream.mapValues(value -> {
            try {
                return mapper.readValue(value, MachineEvent.class);
            } catch (Exception e) {
                throw new RuntimeException("Failed to parse JSON", e);
            }
        });


        // 2. Store status in RocksDB
        parsedStream.process(() -> new AbstractProcessor<String, MachineEvent>() {
            private KeyValueStore<String, String> store;

            @Override
            public void init(ProcessorContext context) {
                // Initialize store using context
                store = (KeyValueStore<String, String>) context.getStateStore(storeName);
            }

            @Override
            public void process(String key, MachineEvent event) {
                if (event.getStatus() != null) {
                    // Write to the state store (RocksDB)
                    store.put(event.getMachineId(), event.getStatus());
                }
            }
            /*
            How This Works
store Initialization:

When the processor is initialized (via the init() method), it fetches the machine-status-store using the context.getStateStore(storeName) call. This makes the RocksDB store available within the processor for read and write operations.

process() Method:

In the process() method, you are writing to the store with store.put(event.getMachineId(), event.getStatus()). This stores the latest status of a machine (event.getStatus()) for a specific machine (event.getMachineId()). It essentially updates the status for each machine as new events come in.
             */
        }, storeName);


        // 3. Filter: Only events with intensity > 60
        KStream<String, MachineEvent> filtered = parsedStream.filter(
                (key, event) -> event.getIntensity() > 60
        );

        // 4. Transform: Add derived riskLevel
        KStream<String, String> transformed = parsedStream.mapValues(event -> {
            Map<String, Object> result = new HashMap<>();
            result.put("machineId", event.getMachineId());
            result.put("intensity", event.getIntensity());
            result.put("temperature", event.getTemperature());
            result.put("riskLevel", event.getTemperature() > 90 ? "HIGH" : "NORMAL");
            try {
                return mapper.writeValueAsString(result);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Failed to serialize JSON", e);
            }
        });

        // 4. Send filtered and transformed streams to different topics
        filtered.mapValues(event -> toJson(event)).to("filtered-topic");
        transformed.to("transformed-topic");

        return stream;
    }

    private String toJson(Object data) {
        try {
            return mapper.writeValueAsString(data);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Bean
    public KafkaStreamsInteractiveQueryService kafkaStreamsInteractiveQueryService(
            StreamsBuilderFactoryBean factoryBean) {
        return new KafkaStreamsInteractiveQueryService(factoryBean);
    }

}
