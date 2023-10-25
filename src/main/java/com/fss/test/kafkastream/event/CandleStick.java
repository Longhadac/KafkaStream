package com.fss.test.kafkastream.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor
@Builder
@Data
public class CandleStick {
    public String symbol;
    public double startPrice;
    public double endPrice;
    public double highPrice;
    public double lowPrice;
    public long startAggregationPeriodTimestamp;
    public long endAggregationPeriodTimestamp;
    public String endTimeString;
    public long aggregationCount;
    public double volume;

    public CandleStick() {
        aggregationCount=0;
    }

    public void aggregateTransLog(TransLog transLog, String key) {
        log.info("New event: translog: {} ; key: {}", transLog, key);
        symbol = key;
        double unitPrice = transLog.getFormattedMatchPrice();
        if (aggregationCount == 0) {
            startAggregationPeriodTimestamp = transLog.ConvertTransLogTime();
            endAggregationPeriodTimestamp = transLog.ConvertTransLogTime();
            startPrice = unitPrice;
            endPrice = unitPrice;
            lowPrice = unitPrice;
            highPrice = unitPrice;
            endTimeString = transLog.tradingdate +" " + transLog.formattedTime;//dd/MM/yyyy HH:mm:ss
        }

        if (startAggregationPeriodTimestamp > transLog.ConvertTransLogTime()) {
            startAggregationPeriodTimestamp = transLog.ConvertTransLogTime();
            startPrice = unitPrice;
        }

        if (endAggregationPeriodTimestamp < transLog.ConvertTransLogTime()) {
            endAggregationPeriodTimestamp = transLog.ConvertTransLogTime();
            endPrice = unitPrice;
            endTimeString = transLog.tradingdate +" " + transLog.formattedTime;
        }

        highPrice = Math.max(highPrice, unitPrice);
        lowPrice = Math.min(lowPrice, unitPrice);
        ++aggregationCount;
        volume += Double.parseDouble(transLog.getFormattedVol());
    }

    @Override
    public String toString() {
        return "CandleStick{" +
                "symbol='" + symbol + '\'' +
                ", startPrice=" + startPrice +
                ", endPrice=" + endPrice +
                ", highPrice=" + highPrice +
                ", lowPrice=" + lowPrice +
                ", startAggregationPeriodTimestamp=" + startAggregationPeriodTimestamp +
                ", endAggregationPeriodTimestamp=" + endAggregationPeriodTimestamp +
                ", aggregationCount=" + aggregationCount +
                ", volume=" + volume +
                '}';
    }
}
