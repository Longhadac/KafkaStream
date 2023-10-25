package com.fss.test.kafkastream.event;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TransLog {
    public String sequenceMsg;
    public String tradingdate;
    public String symbol;
    public String formattedTime;
    public String lastColor;

    public Double formattedMatchPrice;
    public String formattedChangeValue;
    public String formattedVol;
    public String formattedAccVol;
    public String formattedAccVal;

    public long ConvertTransLogTime(){
        final SimpleDateFormat formatter  = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        try {
            Date date = formatter.parse(tradingdate +" " + formattedTime);
            return date.getTime();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
}
