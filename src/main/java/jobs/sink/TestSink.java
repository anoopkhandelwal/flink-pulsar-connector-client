package jobs.sink;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

@Slf4j
public class TestSink extends RichSinkFunction<String> {
    @Override
    public void invoke(String entity, Context flinkContext) throws Exception {
        log.info("Sink = {}", entity);
    }

}
