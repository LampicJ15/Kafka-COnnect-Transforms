package si.hekovnik.transform;


import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import si.hekovnik.utils.JsonConverter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FilterTransformTest {


    @Test
    //returns the the whole message because the condition stands, filter value is a list
    public void filterValueTrue() throws IOException {
        //define the configuration
        Map<String, String> config = new HashMap<>();
        config.put(FilterTransformConfig.FIELD_CONFIG, "payload.after.labels");
        config.put(FilterTransformConfig.FIELD_VALUE, "Kg");

        //define value
        String json = "{\"meta\":{\"timestamp\":1572361327791,\"username\":\"neo4j\",\"txId\":17,\"txEventId\":0,\"txEventsCount\":1,\"operation\":\"created\",\"source\":{\"hostname\":\"neo\"}},\"payload\":{\"id\":\"20\",\"before\":null,\"after\":{\"properties\":{\"name\":\"123abc123\"},\"labels\":[\"Kg\"]},\"type\":\"node\"},\"schema\":{\"properties\":{\"name\":\"String\"},\"constraints\":[]}}";
        Map<String, Object> map = JsonConverter.jsonStringToMap(json);


        //define the FilterTransform
        FilterTransform<SourceRecord> filterValue = new FilterTransform.Value<SourceRecord>();
        filterValue.configure(config);

        //get config
        Assert.assertFalse(filterValue.config().equals(config));

        SourceRecord newRecord = filterValue.apply(new SourceRecord(null, null, "topic", null, null, map));
        filterValue.close();
        System.out.println(newRecord.toString());
    }


    @Test
    //returns null because the condition is false (the field does not contain the given value)
    public void filterValueFalse() throws IOException {
        //define the configuration
        Map<String, String> config = new HashMap<>();
        config.put(FilterTransformConfig.FIELD_CONFIG, "payload.after.labels");
        config.put(FilterTransformConfig.FIELD_VALUE, "Not Kg");

        //define value
        String json = "{\"meta\":{\"timestamp\":1572361327791,\"username\":\"neo4j\",\"txId\":17,\"txEventId\":0,\"txEventsCount\":1,\"operation\":\"created\",\"source\":{\"hostname\":\"neo\"}},\"payload\":{\"id\":\"20\",\"before\":null,\"after\":{\"properties\":{\"name\":\"123abc123\"},\"labels\":[\"Kg\"]},\"type\":\"node\"},\"schema\":{\"properties\":{\"name\":\"String\"},\"constraints\":[]}}";
        Map<String, Object> map = JsonConverter.jsonStringToMap(json);


        //define the FilterTransform
        FilterTransform<SourceRecord> filterValue = new FilterTransform.Value<SourceRecord>();
        filterValue.configure(config);
        SourceRecord newRecord = filterValue.apply(new SourceRecord(null, null, "topic", null, null, map));
        System.out.println(newRecord.toString());
    }

    @Test
    //returns exception since the given field does not exist
    public void noGivenFilter() throws IOException {
        //define the configuration
        Map<String, String> config = new HashMap<>();
        config.put(FilterTransformConfig.FIELD_CONFIG, "payload.false");
        config.put(FilterTransformConfig.FIELD_VALUE, "Kg");

        //define value
        String json = "{\"meta\":{\"timestamp\":1572361327791,\"username\":\"neo4j\",\"txId\":17,\"txEventId\":0,\"txEventsCount\":1,\"operation\":\"created\",\"source\":{\"hostname\":\"neo\"}},\"payload\":{\"id\":\"20\",\"before\":null,\"after\":{\"properties\":{\"name\":\"123abc123\"},\"labels\":[\"Kg\"]},\"type\":\"node\"},\"schema\":{\"properties\":{\"name\":\"String\"},\"constraints\":[]}}";
        Map<String, Object> map = JsonConverter.jsonStringToMap(json);


        //define the FilterTransform
        FilterTransform<SourceRecord> filterValue = new FilterTransform.Value<SourceRecord>();
        filterValue.configure(config);
        try {
            SourceRecord newRecord = filterValue.apply(new SourceRecord(null, null, "topic", null, null, map));
        } catch (IllegalArgumentException e) {
        }
    }


    @Test
    //throws and exceptions because the input is not an array
    public void convertToMapTest() {
        //define the configuration
        Map<String, String> config = new HashMap<>();
        config.put(FilterTransformConfig.FIELD_CONFIG, "payload.after.labels");
        config.put(FilterTransformConfig.FIELD_VALUE, "Not Kg");


        List<String> falseArray = new ArrayList<>();
        falseArray.add("a");
        falseArray.add("b");
        falseArray.add("c");

        //define the FilterTransform
        FilterTransform<SourceRecord> filterValue = new FilterTransform.Value<SourceRecord>();
        filterValue.configure(config);
        try {
            SourceRecord newRecord = filterValue.apply(new SourceRecord(null, null, "topic", null, null, falseArray));
        } catch (DataException e) {
        }
    }

    @Test
    //returns the the whole message because the condition stands, filter value is a string
    public void filterStringValueTrue() throws IOException {
        //define the configuration
        Map<String, String> config = new HashMap<>();
        config.put(FilterTransformConfig.FIELD_CONFIG, "meta.username");
        config.put(FilterTransformConfig.FIELD_VALUE, "neo4j");

        //define value
        String json = "{\"meta\":{\"timestamp\":1572361327791,\"username\":\"neo4j\",\"txId\":17,\"txEventId\":0,\"txEventsCount\":1,\"operation\":\"created\",\"source\":{\"hostname\":\"neo\"}},\"payload\":{\"id\":\"20\",\"before\":null,\"after\":{\"properties\":{\"name\":\"123abc123\"},\"labels\":[\"Kg\"]},\"type\":\"node\"},\"schema\":{\"properties\":{\"name\":\"String\"},\"constraints\":[]}}";
        Map<String, Object> map = JsonConverter.jsonStringToMap(json);


        //define the FilterTransform
        FilterTransform<SourceRecord> filterValue = new FilterTransform.Value<SourceRecord>();
        filterValue.configure(config);

        //get config
        Assert.assertFalse(filterValue.config().equals(config));

        SourceRecord newRecord = filterValue.apply(new SourceRecord(null, null, "topic", null, null, map));
        filterValue.close();
        System.out.println(newRecord.toString());
    }

    @Test
    //returns the the whole message because the condition stands, filter value is an integer
    public void filterIntegerValueTrue() throws IOException {
        //define the configuration
        Map<String, String> config = new HashMap<>();
        config.put(FilterTransformConfig.FIELD_CONFIG, "meta.txId");
        config.put(FilterTransformConfig.FIELD_VALUE, "17");

        //define value
        String json = "{\"meta\":{\"timestamp\":1572361327791,\"username\":\"neo4j\",\"txId\":17,\"txEventId\":0,\"txEventsCount\":1,\"operation\":\"created\",\"source\":{\"hostname\":\"neo\"}},\"payload\":{\"id\":\"20\",\"before\":null,\"after\":{\"properties\":{\"name\":\"123abc123\"},\"labels\":[\"Kg\"]},\"type\":\"node\"},\"schema\":{\"properties\":{\"name\":\"String\"},\"constraints\":[]}}";
        Map<String, Object> map = JsonConverter.jsonStringToMap(json);


        //define the FilterTransform
        FilterTransform<SourceRecord> filterValue = new FilterTransform.Value<SourceRecord>();
        filterValue.configure(config);

        SourceRecord newRecord = filterValue.apply(new SourceRecord(null, null, "topic", null, null, map));
        System.out.println(newRecord.toString());
    }

    @Test
    //returns the the whole message because the condition stands, filter value is a Long
    public void filterLongValueTrue() throws IOException {
        //define the configuration
        Map<String, String> config = new HashMap<>();
        config.put(FilterTransformConfig.FIELD_CONFIG, "meta.timestamp");
        config.put(FilterTransformConfig.FIELD_VALUE, "1572361327791");

        //define value
        String json = "{\"meta\":{\"timestamp\":1572361327791,\"username\":\"neo4j\",\"txId\":17,\"txEventId\":0,\"txEventsCount\":1,\"operation\":\"created\",\"source\":{\"hostname\":\"neo\"}},\"payload\":{\"id\":\"20\",\"before\":null,\"after\":{\"properties\":{\"name\":\"123abc123\"},\"labels\":[\"Kg\"]},\"type\":\"node\"},\"schema\":{\"properties\":{\"name\":\"String\"},\"constraints\":[]}}";
        Map<String, Object> map = JsonConverter.jsonStringToMap(json);

        //define the FilterTransform
        FilterTransform<SourceRecord> filterValue = new FilterTransform.Value<SourceRecord>();
        filterValue.configure(config);

        SourceRecord newRecord = filterValue.apply(new SourceRecord(null, null, "topic", null, null, map));
        System.out.println(newRecord.toString());
    }

    @Test
    public void example() throws IOException {
        //define the configuration
        Map<String, String> config = new HashMap<>();
        config.put(FilterTransformConfig.FIELD_CONFIG, "lastName");
        config.put(FilterTransformConfig.FIELD_VALUE, "LastName1");

        //define value
        String json ="{\"firstName\": \"FirstName1\", \"lastName\": \"LastName1\", \"age\": 30}";
        Map<String, Object> map = JsonConverter.jsonStringToMap(json);


        //define the FilterTransform
        FilterTransform<SourceRecord> filterValue = new FilterTransform.Value<SourceRecord>();
        filterValue.configure(config);

        //get config
        Assert.assertFalse(filterValue.config().equals(config));

        SourceRecord newRecord = filterValue.apply(new SourceRecord(null, null, "topic", null, null, map));
        filterValue.close();
        System.out.println(newRecord.toString());
    }

    @Test
    public void example2() throws IOException {
        //define the configuration
        Map<String, String> config = new HashMap<>();
        config.put(FilterTransformConfig.FIELD_CONFIG, "Person.lastName");
        config.put(FilterTransformConfig.FIELD_VALUE, "LastName1");

        //define value
        String json ="{\"Person\": {\"firstName\": \"FirstName1\", \"lastName\": \"LastName1\", \"age\": 30}}";
        Map<String, Object> map = JsonConverter.jsonStringToMap(json);


        //define the FilterTransform
        FilterTransform<SourceRecord> filterValue = new FilterTransform.Value<SourceRecord>();
        filterValue.configure(config);

        //get config
        Assert.assertFalse(filterValue.config().equals(config));

        SourceRecord newRecord = filterValue.apply(new SourceRecord(null, null, "topic", null, null, map));
        filterValue.close();
        System.out.println(newRecord.toString());
    }

    @Test
    public void filterValueNoField() throws IOException {
        //define the configuration
        Map<String, String> config = new HashMap<>();
        config.put(FilterTransformConfig.FIELD_CONFIG, "payload.after.labels");
        config.put(FilterTransformConfig.FIELD_VALUE, "Kg");
        config.put(FilterTransformConfig.SECONDARY_FIELD_CONFIG, "payload.before.labels");

        //define value
        String json = "{\"meta\":{\"timestamp\":1572427419869,\"username\":\"neo4j\",\"txId\":40,\"txEventId\":5,\"txEventsCount\":7,\"operation\":\"deleted\",\"source\":{\"hostname\":\"neo\"}},\"payload\":{\"id\":\"45\",\"before\":{\"properties\":{\"name\":\"elastic\"},\"labels\":[\"Kg\",\"NotKg\"]},\"after\":null,\"type\":\"node\"},\"schema\":{\"properties\":{\"name\":\"String\"},\"constraints\":[]}}";
        Map<String, Object> map = JsonConverter.jsonStringToMap(json);

        //define the FilterTransform
        FilterTransform<SourceRecord> filterValue = new FilterTransform.Value<SourceRecord>();
        filterValue.configure(config);

        //get config
        Assert.assertFalse(filterValue.config().equals(config));

        SourceRecord newRecord = filterValue.apply(new SourceRecord(null, null, "topic", null, null, map));
        filterValue.close();
        System.out.println(newRecord.toString());
    }


}