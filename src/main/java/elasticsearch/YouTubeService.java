package elasticsearch;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.youtube.YouTube;
import com.google.api.services.youtube.YouTubeScopes;
import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Based on https://github.com/youtube/api-samples/blob/master/java/src/main/java/com/google/api/services/samples/youtube/cmdline/data/Quickstart.java
 */
public class YouTubeService {
    private static final String YOUTUBE_PUSH_NOTIFICATIONS_URL = "https://pubsubhubbub.appspot.com/subscribe";
    private static Logger LOG = LoggerFactory.getLogger(YouTubeService.class);

    /** Application name. */
    private static final String APPLICATION_NAME = "realtime-brand-detection";

    /** Directory to store user credentials for this application. */
    private static final java.io.File DATA_STORE_DIR = new java.io.File(
            // user.dir is the working dir
        System.getProperty("user.dir"), ".credentials/youtube-realtime-brand-detection");

    /** Global instance of the {@link FileDataStoreFactory}. */
    private static FileDataStoreFactory DATA_STORE_FACTORY;

    /** Global instance of the JSON factory. */
    private static final JsonFactory JSON_FACTORY =
        JacksonFactory.getDefaultInstance();

    /** Global instance of the HTTP transport. */
    private static HttpTransport HTTP_TRANSPORT;

    /** Global instance of the scopes required by this quickstart.
     *
     * If modifying these scopes, delete your previously saved credentials
     * at ~/.credentials/drive-java-quickstart
     */
    private static final List<String> SCOPES =
        Arrays.asList(YouTubeScopes.YOUTUBE_READONLY);

    /**
     * Pattern to identify channel ids in strings
     */
    private static Pattern channelIdPattern = Pattern.compile("UC[\\w-]+");


    private static HttpRequestInitializer setHttpTimeout(final HttpRequestInitializer requestInitializer) {
        return new HttpRequestInitializer() {
            @Override
            public void initialize(HttpRequest httpRequest) throws IOException {
                requestInitializer.initialize(httpRequest);
                httpRequest.setConnectTimeout(3 * 60000);  // 3 minutes connect timeout
                httpRequest.setReadTimeout(3 * 60000);  // 3 minutes read timeout
            }
        };
    }

    static {
            try {
            HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
            DATA_STORE_FACTORY = new FileDataStoreFactory(DATA_STORE_DIR);
        } catch (Throwable t) {
            t.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Create an authorized Credential object.
     * @return an authorized Credential object.
     * @throws IOException
     */
    public static Credential authorize(String scopes, String credentialsFilePath) throws IOException {
        // Load client secrets.
        LOG.info("Loading YouTube credentials from {}", credentialsFilePath);
        InputStream in =
            YouTubeService.class.getResourceAsStream(credentialsFilePath);
        Credential credential = GoogleCredential.fromStream(in).createScoped(Collections.singleton(scopes));
        return credential;
    }

    /**
     * Build and return an authorized API client service, such as a YouTube
     * Data API client service.
     * @return an authorized API client service
     * @throws IOException
     */
    public static YouTube getYouTubeService(String scopes, String credentialsFilePath) throws IOException {
        Credential credential = authorize(scopes, credentialsFilePath);
        return new YouTube.Builder(HTTP_TRANSPORT, JSON_FACTORY, setHttpTimeout(credential))
                .setApplicationName(APPLICATION_NAME)
                .build();
    }

    /**
     * Subscribes to video updates for the given channel. https://developers.google.com/youtube/v3/guides/push_notifications
     * @param channelId Identifier of the channel we want to receive video updates from
     * @param callbackUrl The webhook we want YouTube to send the notifications to
     * @throws IOException
     * @throws IllegalArgumentException
     */
    public static void subscribeToChannelVideosPushNotifications(String channelId, String callbackUrl)
            throws IOException, IllegalArgumentException {
        OkHttpClient client = new OkHttpClient();
        String topicUrl = String.format("https://www.youtube.com/xml/feeds/videos.xml?channel_id=%s", channelId);
        String oneWeekInSeconds = String.valueOf(Duration.ofDays(7).getSeconds());

        RequestBody formBody = new FormBody.Builder()
                .add("hub.callback", callbackUrl)
                .add("hub.topic", topicUrl)
                // Verify type
                .add("hub.verify", "async")
                // Subscribe mode
                .add("hub.mode", "subscribe")
                // Optional verify token
//                .add("hub.verify_token", "subscribe")
                // Optional HMAC secret
//                .add("hub.secret", "subscribe")
                // Optional Lease seconds
                .add("hub.lease_seconds", oneWeekInSeconds)
                .build();
        Request request = new Request.Builder()
                .url(YOUTUBE_PUSH_NOTIFICATIONS_URL)
                .post(formBody)
                .build();

        try {
            Response response = client.newCall(request).execute();
            if (!response.isSuccessful()) {
                String error = String.format("The subscription request for the channel %s" +
                        " returned a code different from 2XX: %s", channelId, response.code());
                throw new IllegalArgumentException(error);
            }
        } catch (IOException e) {
            LOG.error("There was an error subscribing the channel {} to the callback url {}", channelId, callbackUrl);
            e.printStackTrace();
            throw e;
        }
    }

    /**
     * Finds the a channel id in the text because sometimes the channel id comes in quotes or the string is not a channel id at all
     * @param text
     * @return A clean channel id like UCucWfbynOLziRO3PAKnMwXQ or null if a channel id is not found
     */
    public static String getChannelIdFromString(String text) {
        if (text == null) {
            return null;
        }

        Matcher matcher = channelIdPattern.matcher(text);
        if (matcher.find()) {
            return matcher.group();
        }

        return null;
    }
}
