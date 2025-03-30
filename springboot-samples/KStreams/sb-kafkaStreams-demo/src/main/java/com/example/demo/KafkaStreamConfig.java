package com.example.demo;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Locale;
import java.util.Properties;

@Configuration // Configuration class for Kafka Streams
public class KafkaStreamConfig {

    @Bean // Defines Kafka Streams bean
    public KafkaStreams kafkaStreams(StreamsBuilder streamsBuilder) {
        Properties props = new Properties();

        // Kafka Streams application properties
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), props);
        streams.start();
        return streams;
    }

    @Bean // Defines a StreamsBuilder bean with transformation logic
    public StreamsBuilder streamsBuilder() {
        StreamsBuilder builder = new StreamsBuilder();

        // Define a stream from "input-topic"
        KStream<String, String> stream = builder.stream("input-topic");

        // Transformation logic: Convert values to uppercase using default locale
        stream.mapValues(value -> value.toUpperCase(Locale.ROOT))
                .to("output-topic");

        return builder;
    }
}

//@Configuration
//public class KafkaStreamConfig {
//
//    @Bean // Defines this method as a Spring Bean, so it runs at application startup
//    public KafkaStreams kafkaStreams() {
//        Properties props = new Properties();
//
//        // Unique identifier for this Kafka Streams application
//        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-app");
//
//        // Kafka broker address
//        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//
//        // Default key serializer/deserializer (serde) - String format
//        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//
//        // Default value serializer/deserializer (serde) - String format
//        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//
//        // Define the Kafka Streams processing logic
//        StreamsBuilder builder = new StreamsBuilder();
//
//        // Read messages from "input-topic"
//        KStream<String, String> stream = builder.stream("input-topic");
//
//        // Transformation: Convert message values to uppercase and send to "output-topic"
//        stream
//                .mapValues(value -> value.toUpperCase()) // Converts message values to uppercase
//                .to("output-topic"); // Writes transformed messages to "output-topic"
//
//        // Create and start the Kafka Streams application
//        KafkaStreams streams = new KafkaStreams(builder.build(), props);
//        streams.start();
//
//        return streams; // Return the KafkaStreams instance
//    }
//}
