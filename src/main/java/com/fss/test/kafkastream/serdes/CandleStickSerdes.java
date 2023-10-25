package com.fss.test.kafkastream.serdes;




import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import com.fss.test.kafkastream.event.CandleStick;

/**
 * Requires the WrapperSerdes to allow this to be added as the default serdes config in the KafkaStreams configuration.
 */
public final class CandleStickSerdes extends Serdes.WrapperSerde<CandleStick> {

    public CandleStickSerdes() {
        super(new JsonSerializer<>(), new JsonDeserializer<>(CandleStick.class));
    }

    public static Serde<CandleStick> serdes() {
        JsonSerializer<CandleStick> serializer = new JsonSerializer<>();
        JsonDeserializer<CandleStick> deserializer = new JsonDeserializer<>(CandleStick.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }
}
