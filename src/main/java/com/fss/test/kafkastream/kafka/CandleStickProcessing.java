package com.fss.test.kafkastream.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import com.fss.test.kafkastream.event.CandleStick;

@Slf4j
@Service
public class CandleStickProcessing {
    public void addNewBar(String key, CandleStick candleStick) {
        synchronized (candleStick.symbol){
            log.warn("Add new: key: {}; candle: {}", key, candleStick);
        }
    }
    public void updateLastBar(CandleStick candleStick){
        synchronized (candleStick.symbol) {
            log.warn("Update last bar: {}", candleStick);
        }
    }
}