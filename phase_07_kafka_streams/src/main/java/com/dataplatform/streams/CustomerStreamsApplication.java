package com.dataplatform.streams;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

public class CustomerStreamsApplication {

    private static final String INPUT_TOPIC =
            "platformdb.public.customers";

    private static final String BUSINESS_OUTPUT_TOPIC =
            "customer-business-events";

    private static final String CITY_COUNT_OUTPUT_TOPIC =
            "customer-count-by-city";

    private static final ObjectMapper OBJECT_MAPPER =
            new ObjectMapper();

    public static void main(String[] args) {

        String bootstrapServers =
                System.getenv().getOrDefault(
                        "KAFKA_BOOTSTRAP_SERVERS",
                        "localhost:9092");

        Properties properties = new Properties();

        properties.put(
                StreamsConfig.APPLICATION_ID_CONFIG,
                "customer-streams");

        properties.put(
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers);

        properties.put(
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.String().getClass());

        properties.put(
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                Serdes.String().getClass());

        properties.put(
                StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
                StreamsConfig.EXACTLY_ONCE_V2);

        properties.put(
                StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG,
                1);

        properties.put(
                StreamsConfig.REPLICATION_FACTOR_CONFIG,
                3);

        properties.put(
                StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
                StreamsConfig.EXACTLY_ONCE_V2);

        properties.put(
                StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG,
                1);

        properties.put(
                StreamsConfig.REPLICATION_FACTOR_CONFIG,
                3);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> cdcEvents =
                builder.stream(
                        INPUT_TOPIC,
                        Consumed.with(
                                Serdes.String(),
                                Serdes.String()));

        /*
         * Stateless business-event pipeline.
         */
        KStream<String, String> businessEvents =
                cdcEvents
                        .filter((key, value) -> value != null)
                        .mapValues(CustomerStreamsApplication::toBusinessEvent)
                        .filter((key, value) -> value != null)
                        .peek((key, value) ->
                                System.out.println(
                                        "BUSINESS EVENT key=" +
                                        key +
                                        " value=" +
                                        value));

        businessEvents.to(
                BUSINESS_OUTPUT_TOPIC,
                Produced.with(
                        Serdes.String(),
                        Serdes.String()));

        /*
         * Stateful pipeline:
         *
         * Business event
         *      -> extract city
         *      -> groupBy city
         *      -> count
         *      -> persistent state store
         *      -> output KTable changes
         */
        KTable<String, Long> customersByCity =
                businessEvents
                        .filter((key, value) ->
                                isCustomerCreated(value))
                        .selectKey((key, value) ->
                                extractCity(value))
                        .filter((city, value) ->
                                city != null &&
                                !city.isBlank())
                        .groupByKey(
                                Grouped.with(
                                        Serdes.String(),
                                        Serdes.String()))
                        .count(
                                Materialized.as(
                                        "customer-count-by-city-store"));

        customersByCity
                .toStream()
                .peek((city, count) ->
                        System.out.println(
                                "CITY COUNT city=" +
                                city +
                                " count=" +
                                count))
                .to(
                        CITY_COUNT_OUTPUT_TOPIC,
                        Produced.with(
                                Serdes.String(),
                                Serdes.Long()));

        Topology topology = builder.build();

        System.out.println(
                "===== Kafka Streams topology =====");

        System.out.println(
                topology.describe());

        KafkaStreams streams =
                new KafkaStreams(
                        topology,
                        properties);

        CountDownLatch latch =
                new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> {
                    System.out.println(
                            "===== Stopping Kafka Streams =====");

                    streams.close();
                    latch.countDown();
                }));

        try {

            System.out.println(
                    "===== Starting Kafka Streams =====");

            System.out.println(
                    "Bootstrap servers: " +
                    bootstrapServers);

            streams.start();

            latch.await();

        } catch (Throwable e) {

            System.err.println(
                    "Kafka Streams failed:");

            e.printStackTrace();

            System.exit(1);
        }
    }

    private static String toBusinessEvent(
            String debeziumEvent) {

        try {

            Map<String, Object> root =
                    OBJECT_MAPPER.readValue(
                            debeziumEvent,
                            new TypeReference<
                                    Map<String, Object>>() {});

            String operation =
                    (String) root.get("op");

            Object afterObject =
                    root.get("after");

            if (afterObject == null) {
                return null;
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> after =
                    (Map<String, Object>) afterObject;

            @SuppressWarnings("unchecked")
            Map<String, Object> source =
                    (Map<String, Object>) root.get("source");

            String eventType =
                    switch (operation) {
                        case "c", "r" ->
                                "CUSTOMER_CREATED";
                        case "u" ->
                                "CUSTOMER_UPDATED";
                        default ->
                                "CUSTOMER_UNKNOWN";
                    };

            Map<String, Object> businessEvent =
                    Map.of(
                            "eventType",
                            eventType,
                            "customerId",
                            after.get("id"),
                            "firstName",
                            after.get("first_name"),
                            "lastName",
                            after.get("last_name"),
                            "email",
                            after.get("email"),
                            "city",
                            after.get("city"),
                            "sourceOperation",
                            operation,
                            "sourceDatabase",
                            source.get("db"),
                            "sourceTable",
                            source.get("table"),
                            "eventTimestamp",
                            root.get("ts_ms"));

            return OBJECT_MAPPER.writeValueAsString(
                    businessEvent);

        } catch (Exception e) {

            System.err.println(
                    "ERROR transforming CDC event: " +
                    e.getMessage());

            return null;
        }
    }

    private static boolean isCustomerCreated(
            String businessEvent) {

        try {

            Map<String, Object> event =
                    OBJECT_MAPPER.readValue(
                            businessEvent,
                            new TypeReference<
                                    Map<String, Object>>() {});

            return "CUSTOMER_CREATED".equals(
                    event.get("eventType"));

        } catch (Exception e) {

            return false;
        }
    }

    private static String extractCity(
            String businessEvent) {

        try {

            Map<String, Object> event =
                    OBJECT_MAPPER.readValue(
                            businessEvent,
                            new TypeReference<
                                    Map<String, Object>>() {});

            Object city =
                    event.get("city");

            return city == null
                    ? null
                    : city.toString();

        } catch (Exception e) {

            return null;
        }
    }
}
