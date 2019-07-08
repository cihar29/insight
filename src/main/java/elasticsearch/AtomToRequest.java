package elasticsearch;

import org.elasticsearch.client.Request;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

public class AtomToRequest {
    //convert xml from YouTube notification to JSON for Elasticsearch index
    public IndexRequest convert(String source, String index) {
        String message;
        try {
            Element entry = getEntryNodeFromXmlString(source);
            String vidId = getText(entry, "yt:videoId");

            return new IndexRequest(index)
                .id(vidId)
                .source(getJSON(entry));

        } catch (ParserConfigurationException | IOException | SAXException e) {
            message = e.getMessage();
            e.printStackTrace();
        } catch (NullPointerException exception) {
            message = String.format("%s%nFind the source below:%n%s", exception.getMessage(), source);
            exception.printStackTrace();
        }
        message = String.format("Couldn't convert atom to IndexRequest:%n%s", message);
        throw new IllegalArgumentException(message);
    }

    private Map<String, Object> getJSON(Element entry) {
        Map<String, Object> jsonMap = new HashMap<>();

        jsonMap.put("channel_id", getText(entry, "yt:channelId"));
        jsonMap.put("video_title", getText(entry, "title"));
        jsonMap.put("video_url", entry.getElementsByTagName("link").item(0).getAttributes().getNamedItem("href").getTextContent());
        jsonMap.put("published", getText(entry, "published"));
        jsonMap.put("updated", getText(entry, "updated"));

        return jsonMap;
    }

    private String getText(Element entry, String tagName) {
        return entry.getElementsByTagName(tagName).item(0).getFirstChild().getTextContent();
    }

    private Element getEntryNodeFromXmlString(String xmlString) throws ParserConfigurationException, IOException, SAXException {
        StringReader reader = new StringReader(xmlString);
        InputSource stream = new InputSource();
        stream.setCharacterStream(reader);
        DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        Document document = builder.parse(stream);
        NodeList entries = document.getElementsByTagName("entry");
        return (Element) entries.item(0);
    }
}
