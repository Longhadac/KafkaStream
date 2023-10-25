package com.fss.test.kafkastream.kafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import static org.apache.kafka.streams.StreamsConfig.*;

@Configuration
public class KafkaConfig {
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    @Value(value = "${test.kafka.stream.id}")
    private String streamId;
    @Value(value = "${test.kafka.backoff.interval}")
    private Long interval;
    @Value(value = "${test.kafka.backoff.max_failure}")
    private Long maxAttempts;//
    @Value(value = "${test.kafka.stream.commit.interval}")
    private Long commitInterval;

    @Bean(name = "onTimeStreamsBuilder")
    public StreamsBuilderFactoryBean onTimeStreamsBuilder() {
        Map<String, Object> props = new HashMap<>();
        props.put(APPLICATION_ID_CONFIG, streamId);
        props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(STATE_DIR_CONFIG,"./rocksdb");
        props.put(DEFAULT_DSL_STORE_CONFIG, StreamsConfig.IN_MEMORY);
        props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(COMMIT_INTERVAL_MS_CONFIG,commitInterval);
        
        var factoryBean = new StreamsBuilderFactoryBean(new KafkaStreamsConfiguration(props));
        return factoryBean;
    }

    @Bean(name = "lateStreamsBuilder")
    public StreamsBuilderFactoryBean lateStreamsBuilder() {
        Map<String, Object> props = new HashMap<>();
        props.put(APPLICATION_ID_CONFIG, "lated_"+streamId);
        props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(STATE_DIR_CONFIG,"./rocksdb");
        props.put(DEFAULT_DSL_STORE_CONFIG, StreamsConfig.IN_MEMORY);
        props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(COMMIT_INTERVAL_MS_CONFIG,commitInterval);
        
        var factoryBean = new StreamsBuilderFactoryBean(new KafkaStreamsConfiguration(props));
        return factoryBean;
    }
}