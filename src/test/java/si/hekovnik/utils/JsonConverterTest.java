package si.hekovnik.utils;


import org.junit.Test;

import java.io.IOException;
import java.util.Map;

public class JsonConverterTest{

    @Test
    public void convertStringToJsonMap() throws IOException {
        String json = "{\"meta\":{\"timestamp\":1572361327791,\"username\":\"neo4j\",\"txId\":17,\"txEventId\":0,\"txEventsCount\":1,\"operation\":\"created\",\"source\":{\"hostname\":\"neo\"}},\"payload\":{\"id\":\"20\",\"before\":null,\"after\":{\"properties\":{\"name\":\"123abc123\"},\"labels\":[\"Kg\"]},\"type\":\"node\"},\"schema\":{\"properties\":{\"name\":\"String\"},\"constraints\":[]}}";
        Map<String,Object> map = JsonConverter.jsonStringToMap(json);
    }

}