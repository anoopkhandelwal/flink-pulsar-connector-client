import jobs.process.TestProcessFunction;
import org.apache.flink.shaded.curator5.com.google.common.collect.Lists;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.Assert;
import org.junit.Test;

public class SampleTest {

    @Test
    public void testFilterElement() throws Exception {

        TestProcessFunction filterFunction = new TestProcessFunction();
        String str = "Hello ";
        OneInputStreamOperatorTestHarness<String, String> testHarness =
                ProcessFunctionTestHarnesses.forProcessFunction(filterFunction);

        testHarness.open();
        testHarness.processElement(str, 1);
        Assert.assertEquals(Lists.newArrayList(new StreamRecord<>("Hello World",1)),
                testHarness.extractOutputStreamRecords());

    }
}
