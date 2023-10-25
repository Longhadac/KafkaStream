# KafkaStream
Using kafka stream to generate candle stick event per minutes from translog:
- 1 stream for ontime translog events
- 1 stream for processing lated translog: update candle stick