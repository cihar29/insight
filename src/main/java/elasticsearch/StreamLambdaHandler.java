package elasticsearch;

import com.amazonaws.serverless.exceptions.ContainerInitializationException;
import com.amazonaws.serverless.proxy.model.AwsProxyRequest;
import com.amazonaws.serverless.proxy.model.AwsProxyResponse;
import com.amazonaws.serverless.proxy.spark.SparkLambdaContainerHandler;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;

import spark.Spark;
import static spark.Spark.get;
import static spark.Spark.post;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//https://github.com/awslabs/aws-serverless-java-container/blob/master/samples/spark/pet-store/src/main/java/com/amazonaws/serverless/sample/spark/StreamLambdaHandler.java
public class StreamLambdaHandler implements RequestStreamHandler {
    private static Logger LOG = LoggerFactory.getLogger(StreamLambdaHandler.class);

    private static SparkLambdaContainerHandler<AwsProxyRequest, AwsProxyResponse> handler;
    static {
        try {
            handler = SparkLambdaContainerHandler.getAwsProxyHandler();

            //Got the subscription request
            get("/notify", (req, res) -> {
                res.type("text/plain");
                res.status(200);

                String challenge = req.queryParams("hub.challenge");
                LOG.info(String.format("Received challenge: %s", challenge));
                return challenge;
            });

            //Got a new YouTube push notification
            post("/notify", (req, res) -> {
                res.type("application/json");
                LOG.info("Received notification");

                String vidId = "";
                try {
                    Elasticsearch es = new Elasticsearch();
                    vidId = es.newRequest(req.body());
                    es.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                res.status(200);
                return vidId + " notified";
            }, new JsonTransformer());

            Spark.awaitInitialization();
        } catch (ContainerInitializationException e) {
            // if we fail here. We re-throw the exception to force another cold start
            e.printStackTrace();
            throw new RuntimeException("Could not initialize Spark container", e);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context)
            throws IOException {
        handler.proxyStream(inputStream, outputStream, context);
    }
}
