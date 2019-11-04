package si.hekovnik.utils;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class MapUtilsTest{

    @Test
    //Test where the field name defines a nested field, returns the list at field "labels"
    public void getValueByField() throws IOException {
        String field = "payload.after.labels";

        String json = "{\"meta\":{\"timestamp\":1572361327791,\"username\":\"neo4j\",\"txId\":17,\"txEventId\":0,\"txEventsCount\":1,\"operation\":\"created\",\"source\":{\"hostname\":\"neo\"}},\"payload\":{\"id\":\"20\",\"before\":null,\"after\":{\"properties\":{\"name\":\"123abc123\"},\"labels\":[\"Kg\"]},\"type\":\"node\"},\"schema\":{\"properties\":{\"name\":\"String\"},\"constraints\":[]}}";
        Map<String,Object> map = JsonConverter.jsonStringToMap(json);

        Object out = MapUtils.getField(map,field,"\\.");
        Assert.assertTrue(((List) out).contains("Kg"));
    }

    @Test
    //Test where the field name defines a first level field, returns a map
    public void getValueByField2() throws IOException {
        String field = "payload";

        String json = "{\"meta\":{\"timestamp\":1572361327791,\"username\":\"neo4j\",\"txId\":17,\"txEventId\":0,\"txEventsCount\":1,\"operation\":\"created\",\"source\":{\"hostname\":\"neo\"}},\"payload\":{\"id\":\"20\",\"before\":null,\"after\":{\"properties\":{\"name\":\"123abc123\"},\"labels\":[\"Kg\"]},\"type\":\"node\"},\"schema\":{\"properties\":{\"name\":\"String\"},\"constraints\":[]}}";
        Map<String,Object> map = JsonConverter.jsonStringToMap(json);

        Object out = MapUtils.getField(map,field,"\\.");
        System.out.println(out);
    }

}