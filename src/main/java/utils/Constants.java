package utils;

public class Constants {
    public static final boolean CHECK_POINTING_ENABLED = true;
    public static final String PULSAR_CLIENT_AUTH_TOKEN = "dummy";
    public static final String PULSAR_HOST = "pulsar+ssl://locahost:6651";
    public static final String PULSAR_ADMIN_HOST = "https://localhost:8443/";
    public static final boolean PULSAR_TLS_ENABLED = true;
    public static final String PULSAR_CERT_PATH = "/usr/local/pulsar-ca.crt";
    public static final Long CHECK_POINT_INTERVAL =  300000L;
    public static final Long PAUSE_TIME_BETWEEN_CHECK_POINTS = 300000L;
    public static final Long CHECK_POINT_TIMEOUT = 200000L;
    public static final int MAX_CONCURRENT_CP = 1;
    public static final int JOB_MAX_PARALLELISM = 2;
    public static final int FLINK_RESTART_ATTEMPTS = 5;
    public static final int FLINK_RESTART_ATTEMPTS_DELAY = 120;
    public static final String SUBSCRIPTION_NAME= "test-subscription-local";
    public static final String CONSUMER_NAME = "test-consumer-local";
    public static final String TOPIC_NAME = "persistent://n1/cc/topic-name";

}
