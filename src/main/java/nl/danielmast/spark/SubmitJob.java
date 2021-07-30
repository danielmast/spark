package nl.danielmast.spark;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.dataproc.v1.Job;
import com.google.cloud.dataproc.v1.JobControllerClient;
import com.google.cloud.dataproc.v1.JobControllerSettings;
import com.google.cloud.dataproc.v1.JobMetadata;
import com.google.cloud.dataproc.v1.JobPlacement;
import com.google.cloud.dataproc.v1.SparkJob;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SubmitJob {
    public static void main(String[] args) throws IOException, InterruptedException {
        String projectId = System.getenv("PROJECT_ID");
        String region = System.getenv("REGION");
        String clusterName = System.getenv("CLUSTER_NAME");
        submitJob(projectId, region, clusterName);
    }

    public static void submitJob(String projectId, String region, String clusterName)
            throws IOException, InterruptedException {
        String myEndpoint = String.format("%s-dataproc.googleapis.com:443", region);

        // Configure the settings for the job controller client.
        JobControllerSettings jobControllerSettings =
                JobControllerSettings.newBuilder().setEndpoint(myEndpoint).build();

        // Create a job controller client with the configured settings. Using a try-with-resources
        // closes the client,
        // but this can also be done manually with the .close() method.
        try (JobControllerClient jobControllerClient =
                     JobControllerClient.create(jobControllerSettings)) {

            // Configure cluster placement for the job.
            JobPlacement jobPlacement = JobPlacement.newBuilder().setClusterName(clusterName).build();

            // Configure Spark job settings.
            SparkJob sparkJob =
                    SparkJob.newBuilder()
                            .setMainClass("org.apache.spark.examples.SparkPi")
                            .addJarFileUris("file:///usr/lib/spark/examples/jars/spark-examples.jar")
                            .addArgs("90000")
                            .build();

            Job job = Job.newBuilder().setPlacement(jobPlacement).setSparkJob(sparkJob).build();

            // Submit an asynchronous request to execute the job.
            OperationFuture<Job, JobMetadata> submitJobAsOperationAsyncRequest =
                    jobControllerClient.submitJobAsOperationAsync(projectId, region, job);

            Job response = submitJobAsOperationAsyncRequest.get();

            // Print output from Google Cloud Storage.
            Matcher matches =
                    Pattern.compile("gs://(.*?)/(.*)").matcher(response.getDriverOutputResourceUri());
            matches.matches();

            Storage storage = StorageOptions.getDefaultInstance().getService();
            Blob blob = storage.get(matches.group(1), String.format("%s.000000000", matches.group(2)));

            System.out.println(
                    String.format("Job finished successfully: %s", new String(blob.getContent())));

        } catch (ExecutionException e) {
            // If the job does not complete successfully, print the error message.
            System.err.println(String.format("submitJob: %s ", e.getMessage()));
        }
    }
}
