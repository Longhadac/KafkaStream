package com.fss.test.kafkastream.serdes;


import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import com.fss.test.kafkastream.event.TransLog;

/**
 * Requires the WrapperSerdes to allow this to be added as the default serdes config in the KafkaStreams configuration.
 */
public final class TranslogSerdes extends Serdes.WrapperSerde<TransLog> {
    public TranslogSerdes() {
        super(new JsonSerializer<>(), new JsonDeserializer<>(TransLog.class));
    }

    public static Serde<TransLog> serdes() {
        JsonSerializer<TransLog> serializer = new JsonSerializer<>();
        JsonDeserializer<TransLog> deserializer = new JsonDeserializer<>(TransLog.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }
}
