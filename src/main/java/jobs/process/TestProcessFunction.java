package jobs.process;

import jobs.type.TestJob;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestProcessFunction extends ProcessFunction<String, String> {
    private static final Logger log = LoggerFactory.getLogger(TestJob.class);

    @Override
    public void processElement(String s, Context context, Collector<String> collector) throws Exception {
        log.info("Message = {}", s);
        Thread.sleep(1000);
        collector.collect(s);
    }
}

