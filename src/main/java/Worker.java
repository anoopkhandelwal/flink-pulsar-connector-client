import jobs.type.TestJob;

public class Worker {
    public static void main(String[] args) {
        TestJob job = new TestJob();
        job.execute();
    }
}
