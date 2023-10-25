package com.fss.test.kafkastream.event;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

@Slf4j
public class TransLogTimeStampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
        try{
            if(record.value() instanceof TransLog){
                return ((TransLog)record.value()).ConvertTransLogTime();
            }
        }catch (Exception ex){
            log.error(ex.toString());
        }
        return -1;
    }
}