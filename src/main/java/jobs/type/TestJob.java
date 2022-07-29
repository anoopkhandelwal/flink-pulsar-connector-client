package jobs.type;
import jobs.process.TestProcessFunction;
import jobs.sink.TestSink;
import lombok.extern.slf4j.Slf4j;
import java.util.*;
import de.javakaffee.kryoserializers.jodatime.JodaDateTimeSerializer;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.pulsar.common.config.PulsarOptions;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.PulsarSourceBuilder;
import org.apache.flink.connector.pulsar.source.PulsarSourceOptions;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.Constants;

import java.util.concurrent.TimeUnit;

public class TestJob{


    private static final Logger log = LoggerFactory.getLogger(TestJob.class);
    private PulsarSource<String> getPulsarSourceFunctionV1(Set<String> topicsString) {
        String authParams = String.format("token:%s", Constants.PULSAR_CLIENT_AUTH_TOKEN);

        Properties properties = new Properties();
        properties.setProperty(PulsarOptions.PULSAR_AUTH_PLUGIN_CLASS_NAME.key(), AuthenticationToken.class.getName());
        properties.setProperty(PulsarOptions.PULSAR_AUTH_PARAMS.key(), authParams);
        properties.setProperty(PulsarOptions.PULSAR_TLS_TRUST_CERTS_FILE_PATH.key(),Constants.PULSAR_CERT_PATH);
        properties.setProperty(PulsarOptions.PULSAR_SERVICE_URL.key(),Constants.PULSAR_HOST);
        properties.setProperty(PulsarOptions.PULSAR_CONNECT_TIMEOUT.key(),"600000");
        properties.setProperty(PulsarOptions.PULSAR_READ_TIMEOUT.key(),"600000");
        properties.setProperty(PulsarOptions.PULSAR_REQUEST_TIMEOUT.key(),"600000");
        properties.setProperty(PulsarSourceOptions.PULSAR_ENABLE_AUTO_ACKNOWLEDGE_MESSAGE.key(),Boolean.TRUE.toString());
        //properties.setProperty(PulsarSourceOptions.PULSAR_ACK_RECEIPT_ENABLED.key(),Boolean.TRUE.toString());
        //properties.setProperty(PulsarSourceOptions.PULSAR_AUTO_COMMIT_CURSOR_INTERVAL.key(),"1000l");

        PulsarSourceBuilder<String> builder = PulsarSource.builder()
                .setServiceUrl(Constants.PULSAR_HOST)
                .setAdminUrl(Constants.PULSAR_ADMIN_HOST)
                .setProperties(properties)
                .setConfig(PulsarSourceOptions.PULSAR_PARTITION_DISCOVERY_INTERVAL_MS,-1L)
                .setStartCursor(StartCursor.earliest())
                .setDeserializationSchema(PulsarDeserializationSchema.flinkSchema(new SimpleStringSchema()))
                .setSubscriptionName(Constants.SUBSCRIPTION_NAME)
                .setSubscriptionType(SubscriptionType.Failover)
                .setConsumerName(Constants.CONSUMER_NAME)
                .setTopics(new ArrayList<>(topicsString));
        return builder.build();

    }

    private DataStream<String> getSourceInputV1(PulsarSource<String> src, StreamExecutionEnvironment env) {
        env.getConfig().setAutoWatermarkInterval(0L);
        env.addDefaultKryoSerializer(DateTime.class, JodaDateTimeSerializer.class);
        String sourceName = String.format("pulsar-source-local");
        return env.fromSource(src, WatermarkStrategy.noWatermarks(),sourceName)
                .setParallelism(1)
                .uid(sourceName)
                .name(sourceName);
    }

    public void execute() {
        try {

            log.info("Starting Test Job.");
            Set<String> suppressionTopics = new HashSet<>();
            String topicPattern = Constants.TOPIC_NAME;
            suppressionTopics.add(topicPattern);
            PulsarSource<String> src = getPulsarSourceFunctionV1(suppressionTopics);

            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            DataStream<String> postgresSource = getSourceInputV1(src, env);
            int defaultParallelism = Constants.JOB_MAX_PARALLELISM;
            postgresSource
                    .process(new TestProcessFunction()).setParallelism(defaultParallelism)
                    .uid("test-job-pf")
                    .name("test-job-pf")
                    .addSink(new TestSink()).setParallelism(1)
                    .uid("test-data-sf")
                    .name("test-data-sf");


            if (Constants.CHECK_POINTING_ENABLED) {
                env.enableCheckpointing(Constants.CHECK_POINT_INTERVAL, CheckpointingMode.AT_LEAST_ONCE);
                env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
                env.getCheckpointConfig().setCheckpointInterval(Constants.CHECK_POINT_INTERVAL);
                env.getCheckpointConfig()
                        .setMinPauseBetweenCheckpoints(Constants.PAUSE_TIME_BETWEEN_CHECK_POINTS);
                env.getCheckpointConfig().setMaxConcurrentCheckpoints(Constants.MAX_CONCURRENT_CP);
                env.getCheckpointConfig().setCheckpointTimeout(Constants.CHECK_POINT_TIMEOUT);
            }
            env.setMaxParallelism(Constants.JOB_MAX_PARALLELISM);
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                    Constants.FLINK_RESTART_ATTEMPTS,
                    Time.of(Constants.FLINK_RESTART_ATTEMPTS_DELAY, TimeUnit.SECONDS)
            ));
            env.execute("test-job-execution");
        } catch (Exception e) {
            log.error("Exception occurred in JobType: {}", e);
        }

    }
}
