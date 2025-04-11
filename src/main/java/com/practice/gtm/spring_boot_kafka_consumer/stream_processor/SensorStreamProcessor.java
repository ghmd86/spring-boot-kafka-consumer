package com.practice.gtm.spring_boot_kafka_consumer.stream_processor;

import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

public class SensorStreamProcessor {
    public static void main(String[] args) {

        Properties streamsConfig = new Properties();

        streamsConfig.put("application.id", "sensor-data-processor");
        streamsConfig.put("bootstrap.servers", "172.31.238.74:9092");
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> kStream = streamsBuilder.stream("sensor-data");

        kStream.groupByKey().windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(30)))
                .aggregate(() -> 0.0,
                        (key, value, aggregate) -> aggregate + Double.parseDouble(value.split(":")[1].trim()),
                Materialized.as("temperature-store")
                ).toStream().map((Windowed<String> key, Double value) -> new KeyValue<>(key.key(), "Average temperature: " + value));
    }
}
