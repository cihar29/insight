package elasticsearch;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.youtube.YouTube;
import com.google.api.services.youtube.YouTubeScopes;
import com.google.api.services.youtube.model.Video;
import com.google.api.services.youtube.model.VideoListResponse;
import com.google.api.services.youtube.model.VideoSnippet;
import com.google.api.services.youtube.model.VideoStatistics;
import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class Update {

    final static String youtubeCredentialsFile = "/cred.json";
    static YouTube youtube;

    public void testUpdate() {
        String testId = "HLToFiqDHF0";
        String testXml =
            "<feed xmlns:yt=\"http://www.youtube.com/xml/schemas/2015\"\n" +
            "         xmlns=\"http://www.w3.org/2005/Atom\">\n" +
            "  <link rel=\"hub\" href=\"https://pubsubhubbub.appspot.com\"/>\n" +
            "  <link rel=\"self\" href=\"https://www.youtube.com/xml/feeds/videos.xml?channel_id=CHANNEL_ID\"/>\n" +
            "  <title>YouTube video feed</title>\n" +
            "  <updated>2015-04-01T19:05:24.552394234+00:00</updated>\n" +
            "  <entry>\n" +
            "    <id>yt:video:" + testId + "</id>\n" +
            "    <yt:videoId>" + testId + "</yt:videoId>\n" +
            "    <yt:channelId>CHANNEL_ID</yt:channelId>\n" +
            "    <title>Video title</title>\n" +
            "    <link rel=\"alternate\" href=\"http://www.youtube.com/watch?v=" + testId + "\"/>\n" +
            "    <author>\n" +
            "     <name>Channel title</name>\n" +
            "     <uri>http://www.youtube.com/channel/CHANNEL_ID</uri>\n" +
            "    </author>\n" +
            "    <published>2015-03-06T21:40:57+00:00</published>\n" +
            "    <updated>2015-03-09T19:05:24.552394234+00:00</updated>\n" +
            "  </entry>\n" +
            "</feed>";

        try {
            System.out.println(testXml);
            Elasticsearch es = new Elasticsearch();
            String id = es.newRequest(testXml);

            es.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Update() {
        try {
            youtube = YouTubeService.getYouTubeService(YouTubeScopes.YOUTUBE_READONLY, youtubeCredentialsFile);
        } catch (Exception e) {
            System.out.println("Could not get credentials");
            e.printStackTrace();
        }
    }

    public Map<String, Object> updateSnippet(String id) throws Exception {
        System.out.println("Getting snippet for " + id);
        Map<String, Object> jsonMap = new HashMap<>();
        try {
            List<Video> list = youtube.videos().list("snippet").setId(id).execute().getItems();

            if (list.size() > 0) {
                VideoSnippet snippet = list.get(0).getSnippet();

                jsonMap.put("category_id", snippet.getCategoryId());
                jsonMap.put("channel_title", snippet.getChannelTitle());
                jsonMap.put("default_language", snippet.getDefaultLanguage());
                jsonMap.put("description", snippet.getDescription());
                jsonMap.put("tags", snippet.getTags());
            }
        //quota is up
        } catch (GoogleJsonResponseException e) {
            System.out.println("Quota is up");
            e.printStackTrace();
        } catch (Throwable t) {
            System.out.println("Can't connect to YouTube");
            t.printStackTrace();
        }
        return jsonMap;
    }

    public Map<String, Object> updateStats(String id) throws Exception {
        System.out.println("Getting statistics for " + id);
        Map<String, Object> jsonMap = new HashMap<>();
        try {
            List<Video> list = youtube.videos().list("statistics").setId(id).execute().getItems();

            if (list.size() > 0) {
                VideoStatistics statistics = list.get(0).getStatistics();

                String updatedAt = ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT);

                jsonMap.put("updated_at", updatedAt);
                jsonMap.put("view_count", statistics.getViewCount() == null ? 0 : statistics.getViewCount().intValue());
                jsonMap.put("like_count", statistics.getLikeCount() == null ? 0 : statistics.getLikeCount().intValue());
                jsonMap.put("dislike_count", statistics.getDislikeCount() == null ? 0 : statistics.getDislikeCount().intValue());
                jsonMap.put("comment_count", statistics.getCommentCount() == null ? 0 : statistics.getCommentCount().intValue());
                jsonMap.put("favorite_count", statistics.getFavoriteCount() == null ? 0 : statistics.getFavoriteCount().intValue());
            }
        //quota is up
        } catch (GoogleJsonResponseException e) {
            System.out.println("Quota is up");
            e.printStackTrace();
        } catch (Throwable t) {
            System.out.println("Can't connect to YouTube");
            t.printStackTrace();
        }
        return jsonMap;
    }
}
