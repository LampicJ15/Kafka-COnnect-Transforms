package si.hekovnik.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class JsonConverter {

    public static Map<String, Object> jsonStringToMap(String json) throws IOException {
        Map<String,Object> map;
        ObjectMapper mapper = new ObjectMapper();

        map = mapper.readValue(json, new TypeReference<HashMap>(){});
        return map;
    }
}