package si.hekovnik.utils;

import java.util.Map;

public class MapUtils {

    public static Object getField(Map<String, Object> map, String inputField, String delimiter) throws NullPointerException{
        String[] fieldPath = inputField.split(delimiter);

        Map<String, Object> tmpMap = map;

        for (int i = 0; i < fieldPath.length - 1; i++) {
            tmpMap = (Map) tmpMap.get(fieldPath[i]);

            if (tmpMap == null){
                throw new NullPointerException("The array does not contain a value for the given key/field: "+ inputField);
            }

        }
        return tmpMap.get(fieldPath[fieldPath.length - 1]);
    }
}
