package com.fss.test.kafkastream.kafka;

import lombok.extern.slf4j.Slf4j;
import lombok.val;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.WindowedSerializer;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.fss.test.kafkastream.event.CandleStick;
import com.fss.test.kafkastream.event.TransLog;
import com.fss.test.kafkastream.event.TransLogTimeStampExtractor;
import com.fss.test.kafkastream.serdes.CandleStickSerdes;
import com.fss.test.kafkastream.serdes.TranslogSerdes;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import java.time.Duration;

@Component
@Slf4j
public class LatedTopology {
    @Value(value = "${test.kafka.transLogInboundTopic}")
    private String transLogInboundTopic;
    @Value(value = "${test.kafka.lateCandleStickTopic}")
    private String lateCandleStickTopic;

    private static final Serde<String> STRING_SERDE = Serdes.String();
    
    @Resource(name="lateStreamsBuilder")
    StreamsBuilder  lateStreamsBuilder;

    @PostConstruct
    public void buildPipeline() throws InterruptedException {
        val storeSupplier = Stores.inMemoryKeyValueStore("in-mem");
        log.info("Init lated stream");
        lateStreamsBuilder.stream(transLogInboundTopic, Consumed.with(STRING_SERDE, TranslogSerdes.serdes()).withTimestampExtractor(new TransLogTimeStampExtractor()))
        .groupBy((s, transLog) -> transLog.symbol)
        .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMinutes(1), Duration.ofSeconds(59)))
        .aggregate( CandleStick::new,
            (s, transLog, candleStick) -> {
                candleStick.aggregateTransLog(transLog, s);
                return candleStick;
            }
            , Materialized.as(storeSupplier).with(Serdes.String(), CandleStickSerdes.serdes())
        )
        // .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull()))
        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
        .toStream()
        .to(lateCandleStickTopic, Produced.with(getStringWindowedSerde(), CandleStickSerdes.serdes()));
        // candleStickKStream.peek((key,value) -> log.warn("Generate new stick; stick: {}", value));
        lateStreamsBuilder.build();
    }
    
    private static Serde<Windowed<String>> getStringWindowedSerde() {
        StringSerializer stringSerializer = new StringSerializer();
        StringDeserializer stringDeserializer = new StringDeserializer();
        WindowedSerializer<String> windowedSerializer = new TimeWindowedSerializer<>(stringSerializer);
        TimeWindowedDeserializer<String> windowedDeserializer = new TimeWindowedDeserializer<>(stringDeserializer);
        return Serdes.serdeFrom(windowedSerializer,windowedDeserializer);
    }
}