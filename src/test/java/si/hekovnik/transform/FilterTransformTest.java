package si.hekovnik.transform;


import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Assert;
import org.junit.Test;
import si.hekovnik.utils.JsonConverter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FilterTransformTest {

    private Map<String, Object> getConfig(String nodeFieldConfig, String secondNodeFieldConfig, List<String> fieldValues, String relationStartField, String relationEndField) {
        //define the configuration
        Map<String, Object> config = new HashMap<>();

        //node cdc configuration
        config.put(FilterTransformConfig.NODE_FIELD_CONFIG, nodeFieldConfig);
        config.put(FilterTransformConfig.NODE_SECONDARY_FIELD_CONFIG, secondNodeFieldConfig);

        //define the labels you want to subscribe to
        config.put(FilterTransformConfig.LABELS, fieldValues);

        //relationship cdc config
        config.put(FilterTransformConfig.RELATIONSHIP_START_FIELD, relationStartField);
        config.put(FilterTransformConfig.RELATIONSHIP_END_FIELD, relationEndField);

        return config;
    }

    @Test
    /**
     * Sends the node cdc record through because it contains the defined labels (in payload.after)
     */
    public void passNodeRecordAfter() throws IOException {
        //define value
        String json = "{\n" +
                "  \"meta\": {\n" +
                "    \"timestamp\": 1532597182604,\n" +
                "    \"username\": \"neo4j\",\n" +
                "    \"tx_id\": 3,\n" +
                "    \"tx_event_id\": 0,\n" +
                "    \"tx_events_count\": 2,\n" +
                "    \"operation\": \"created\",\n" +
                "    \"source\": {\n" +
                "      \"hostname\": \"neo4j.mycompany.com\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"payload\": {\n" +
                "    \"id\": \"1004\",\n" +
                "    \"type\": \"node\",\n" +
                "    \"after\": {\n" +
                "      \"labels\": [\"Person\"],\n" +
                "      \"properties\": {\n" +
                "        \"email\": \"annek@noanswer.org\",\n" +
                "        \"last_name\": \"Kretchmar\",\n" +
                "        \"first_name\": \"Anne Marie\"\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"schema\": {\n" +
                "    \"properties\": {\n" +
                "      \"last_name\": \"String\",\n" +
                "      \"email\": \"String\",\n" +
                "      \"first_name\": \"String\"\n" +
                "    },\n" +
                "    \"constraints\": [{\n" +
                "      \"label\": \"Person\",\n" +
                "      \"properties\": [\"first_name\", \"last_name\"],\n" +
                "      \"type\": \"UNIQUE\"\n" +
                "    }]\n" +
                "  }\n" +
                "}";

        Map<String, Object> record = JsonConverter.jsonStringToMap(json);

        //define config
        List<String> labels = new ArrayList<>();
        labels.add("Person");
        Map<String, Object> config = getConfig(FilterTransformConfig.NODE_FIELD_CONFIG_DEFAULT, FilterTransformConfig.NODE_SECONDARY_FIELD_CONFIG_DEFAULT, labels, FilterTransformConfig.RELATIONSHIP_START_FIELD_DEFAULT, FilterTransformConfig.RELATIONSHIP_END_FIELD_DEFAULT);

        //define the FilterTransform
        FilterTransform<SourceRecord> filterValue = new FilterTransform.Value<SourceRecord>();
        filterValue.configure(config);

        //get config
        Assert.assertFalse(filterValue.config().equals(config));

        SourceRecord newRecord = filterValue.apply(new SourceRecord(null, null, "topic", null, null, record));
        filterValue.close();
        System.out.println(newRecord.toString());
        Assert.assertTrue(newRecord.value() != null);
    }

    @Test
    /**
     * Filters the node cdc record through because it does not contain the defined labels (in payload.after)
     */
    public void filterNodeRecordAfter() throws IOException {
        //define value
        String json = "{\n" +
                "  \"meta\": {\n" +
                "    \"timestamp\": 1532597182604,\n" +
                "    \"username\": \"neo4j\",\n" +
                "    \"tx_id\": 3,\n" +
                "    \"tx_event_id\": 0,\n" +
                "    \"tx_events_count\": 2,\n" +
                "    \"operation\": \"created\",\n" +
                "    \"source\": {\n" +
                "      \"hostname\": \"neo4j.mycompany.com\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"payload\": {\n" +
                "    \"id\": \"1004\",\n" +
                "    \"type\": \"node\",\n" +
                "    \"after\": {\n" +
                "      \"labels\": [\"Person\"],\n" +
                "      \"properties\": {\n" +
                "        \"email\": \"annek@noanswer.org\",\n" +
                "        \"last_name\": \"Kretchmar\",\n" +
                "        \"first_name\": \"Anne Marie\"\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"schema\": {\n" +
                "    \"properties\": {\n" +
                "      \"last_name\": \"String\",\n" +
                "      \"email\": \"String\",\n" +
                "      \"first_name\": \"String\"\n" +
                "    },\n" +
                "    \"constraints\": [{\n" +
                "      \"label\": \"Person\",\n" +
                "      \"properties\": [\"first_name\", \"last_name\"],\n" +
                "      \"type\": \"UNIQUE\"\n" +
                "    }]\n" +
                "  }\n" +
                "}";

        Map<String, Object> record = JsonConverter.jsonStringToMap(json);

        //define config
        List<String> labels = new ArrayList<>();
        labels.add("Not Person");
        Map<String, Object> config = getConfig(FilterTransformConfig.NODE_FIELD_CONFIG_DEFAULT, FilterTransformConfig.NODE_SECONDARY_FIELD_CONFIG_DEFAULT, labels, FilterTransformConfig.RELATIONSHIP_START_FIELD_DEFAULT, FilterTransformConfig.RELATIONSHIP_END_FIELD_DEFAULT);

        //define the FilterTransform
        FilterTransform<SourceRecord> filterValue = new FilterTransform.Value<SourceRecord>();
        filterValue.configure(config);

        //get config
        Assert.assertFalse(filterValue.config().equals(config));

        SourceRecord newRecord = filterValue.apply(new SourceRecord(null, null, "topic", null, null, record));
        filterValue.close();
        System.out.println(newRecord.toString());
        Assert.assertTrue(newRecord.value() == null);
    }

    @Test
    /**
     * Passes the node cdc record through because it does  contain the defined labels (in payload.before)
     */
    public void passNodeRecordBefore() throws IOException {
        //define value
        String json = "{\n" +
                "        \"meta\": {\n" +
                "        \"timestamp\": 1532597182604,\n" +
                "                \"username\": \"neo4j\",\n" +
                "                \"tx_id\": 3,\n" +
                "                \"tx_event_id\": 0,\n" +
                "                \"tx_events_count\": 2,\n" +
                "                \"operation\": \"created\",\n" +
                "                \"source\": {\n" +
                "            \"hostname\": \"neo4j.mycompany.com\"\n" +
                "        }\n" +
                "    },\n" +
                "        \"payload\": {\n" +
                "        \"id\": \"1004\",\n" +
                "                \"type\": \"node\",\n" +
                "        \"before\": {\n" +
                "            \"labels\": [\"Person\", \"Female\"],\n" +
                "            \"properties\": {\n" +
                "                \"email\": \"annek@noanswer.org\",\n" +
                "                        \"last_name\": \"Kretchmar\",\n" +
                "                        \"first_name\": \"Anne Marie\"\n" +
                "            }\n" +
                "        }\n" +
                "    },\n" +
                "        \"schema\": {\n" +
                "        \"properties\": {\n" +
                "            \"last_name\": \"String\",\n" +
                "                    \"email\": \"String\",\n" +
                "                    \"first_name\": \"String\"\n" +
                "        },\n" +
                "        \"constraints\": [{\n" +
                "            \"label\": \"Person\",\n" +
                "                    \"properties\": [\"first_name\", \"last_name\"],\n" +
                "            \"type\": \"UNIQUE\"\n" +
                "        }]\n" +
                "    }\n" +
                "    }\n" +
                "}";

        Map<String, Object> record = JsonConverter.jsonStringToMap(json);

        //define config
        List<String> labels = new ArrayList<>();
        labels.add("Female");
        labels.add("Person");
        labels.add("PersonExmp");
        Map<String, Object> config = getConfig(FilterTransformConfig.NODE_FIELD_CONFIG_DEFAULT, FilterTransformConfig.NODE_SECONDARY_FIELD_CONFIG_DEFAULT, labels, FilterTransformConfig.RELATIONSHIP_START_FIELD_DEFAULT, FilterTransformConfig.RELATIONSHIP_END_FIELD_DEFAULT);

        //define the FilterTransform
        FilterTransform<SourceRecord> filterValue = new FilterTransform.Value<SourceRecord>();
        filterValue.configure(config);

        //get config
        Assert.assertFalse(filterValue.config().equals(config));

        SourceRecord newRecord = filterValue.apply(new SourceRecord(null, null, "topic", null, null, record));
        filterValue.close();
        System.out.println(newRecord.toString());
        Assert.assertTrue(newRecord.value() != null);
    }

    @Test
    /**
     * Filters the node cdc record because it does not contain the defined labels (in payload.before)
     */
    public void filterNodeRecordBefore() throws IOException {
        //define value
        String json = "{\n" +
                "        \"meta\": {\n" +
                "        \"timestamp\": 1532597182604,\n" +
                "                \"username\": \"neo4j\",\n" +
                "                \"tx_id\": 3,\n" +
                "                \"tx_event_id\": 0,\n" +
                "                \"tx_events_count\": 2,\n" +
                "                \"operation\": \"created\",\n" +
                "                \"source\": {\n" +
                "            \"hostname\": \"neo4j.mycompany.com\"\n" +
                "        }\n" +
                "    },\n" +
                "        \"payload\": {\n" +
                "        \"id\": \"1004\",\n" +
                "                \"type\": \"node\",\n" +
                "        \"before\": {\n" +
                "            \"labels\": [\"PersonNot\", \"FemaleExmp\"],\n" +
                "            \"properties\": {\n" +
                "                \"email\": \"annek@noanswer.org\",\n" +
                "                        \"last_name\": \"Kretchmar\",\n" +
                "                        \"first_name\": \"Anne Marie\"\n" +
                "            }\n" +
                "        }\n" +
                "    },\n" +
                "        \"schema\": {\n" +
                "        \"properties\": {\n" +
                "            \"last_name\": \"String\",\n" +
                "                    \"email\": \"String\",\n" +
                "                    \"first_name\": \"String\"\n" +
                "        },\n" +
                "        \"constraints\": [{\n" +
                "            \"label\": \"Person\",\n" +
                "                    \"properties\": [\"first_name\", \"last_name\"],\n" +
                "            \"type\": \"UNIQUE\"\n" +
                "        }]\n" +
                "    }\n" +
                "    }\n" +
                "}";

        Map<String, Object> record = JsonConverter.jsonStringToMap(json);

        //define config
        List<String> labels = new ArrayList<>();
        labels.add("Female");
        labels.add("Person");
        labels.add("PersonExmp");
        Map<String, Object> config = getConfig(FilterTransformConfig.NODE_FIELD_CONFIG_DEFAULT, FilterTransformConfig.NODE_SECONDARY_FIELD_CONFIG_DEFAULT, labels, FilterTransformConfig.RELATIONSHIP_START_FIELD_DEFAULT, FilterTransformConfig.RELATIONSHIP_END_FIELD_DEFAULT);

        //define the FilterTransform
        FilterTransform<SourceRecord> filterValue = new FilterTransform.Value<SourceRecord>();
        filterValue.configure(config);

        //get config
        Assert.assertFalse(filterValue.config().equals(config));

        SourceRecord newRecord = filterValue.apply(new SourceRecord(null, null, "topic", null, null, record));
        filterValue.close();
        System.out.println(newRecord.toString());
        Assert.assertTrue(newRecord.value() == null);
    }

    @Test
    /**
     * Throws exception no after or before field
     */
    public void excpetionNoNodeField() throws IOException {
        //define value
        String json = "{\n" +
                "        \"meta\": {\n" +
                "        \"timestamp\": 1532597182604,\n" +
                "                \"username\": \"neo4j\",\n" +
                "                \"tx_id\": 3,\n" +
                "                \"tx_event_id\": 0,\n" +
                "                \"tx_events_count\": 2,\n" +
                "                \"operation\": \"created\",\n" +
                "                \"source\": {\n" +
                "            \"hostname\": \"neo4j.mycompany.com\"\n" +
                "        }\n" +
                "    },\n" +
                "        \"payload\": {\n" +
                "        \"id\": \"1004\",\n" +
                "                \"type\": \"node\",\n" +
                "        \"before\": {\n" +
                "            \"labels\": [\"PersonNot\", \"FemaleExmp\"],\n" +
                "            \"properties\": {\n" +
                "                \"email\": \"annek@noanswer.org\",\n" +
                "                        \"last_name\": \"Kretchmar\",\n" +
                "                        \"first_name\": \"Anne Marie\"\n" +
                "            }\n" +
                "        }\n" +
                "    },\n" +
                "        \"schema\": {\n" +
                "        \"properties\": {\n" +
                "            \"last_name\": \"String\",\n" +
                "                    \"email\": \"String\",\n" +
                "                    \"first_name\": \"String\"\n" +
                "        },\n" +
                "        \"constraints\": [{\n" +
                "            \"label\": \"Person\",\n" +
                "                    \"properties\": [\"first_name\", \"last_name\"],\n" +
                "            \"type\": \"UNIQUE\"\n" +
                "        }]\n" +
                "    }\n" +
                "    }\n" +
                "}";

        Map<String, Object> record = JsonConverter.jsonStringToMap(json);

        //define config
        List<String> labels = new ArrayList<>();
        labels.add("Female");
        labels.add("Person");
        labels.add("PersonExmp");
        Map<String, Object> config = getConfig("payload.after.false", "payload.before.false", labels, FilterTransformConfig.RELATIONSHIP_START_FIELD_DEFAULT, FilterTransformConfig.RELATIONSHIP_END_FIELD_DEFAULT);

        //define the FilterTransform
        FilterTransform<SourceRecord> filterValue = new FilterTransform.Value<SourceRecord>();
        filterValue.configure(config);

        //get config
        Assert.assertFalse(filterValue.config().equals(config));

        try {
            SourceRecord newRecord = filterValue.apply(new SourceRecord(null, null, "topic", null, null, record));
        } catch (IllegalArgumentException e) {

        }
    }

    @Test
    /**
     * Sends the node cdc record through because it contains the defined labels (in payload.after), field is string
     */
    public void passNodeRecordAfterString() throws IOException {
        //define value
        String json = "{\n" +
                "  \"meta\": {\n" +
                "    \"timestamp\": 1532597182604,\n" +
                "    \"username\": \"neo4j\",\n" +
                "    \"tx_id\": 3,\n" +
                "    \"tx_event_id\": 0,\n" +
                "    \"tx_events_count\": 2,\n" +
                "    \"operation\": \"created\",\n" +
                "    \"source\": {\n" +
                "      \"hostname\": \"neo4j.mycompany.com\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"payload\": {\n" +
                "    \"id\": \"1004\",\n" +
                "    \"type\": \"node\",\n" +
                "    \"after\": {\n" +
                "      \"labels\": \"Person\",\n" +
                "      \"properties\": {\n" +
                "        \"email\": \"annek@noanswer.org\",\n" +
                "        \"last_name\": \"Kretchmar\",\n" +
                "        \"first_name\": \"Anne Marie\"\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"schema\": {\n" +
                "    \"properties\": {\n" +
                "      \"last_name\": \"String\",\n" +
                "      \"email\": \"String\",\n" +
                "      \"first_name\": \"String\"\n" +
                "    },\n" +
                "    \"constraints\": [{\n" +
                "      \"label\": \"Person\",\n" +
                "      \"properties\": [\"first_name\", \"last_name\"],\n" +
                "      \"type\": \"UNIQUE\"\n" +
                "    }]\n" +
                "  }\n" +
                "}";

        Map<String, Object> record = JsonConverter.jsonStringToMap(json);

        //define config
        List<String> labels = new ArrayList<>();
        labels.add("Person");
        Map<String, Object> config = getConfig(FilterTransformConfig.NODE_FIELD_CONFIG_DEFAULT, FilterTransformConfig.NODE_SECONDARY_FIELD_CONFIG_DEFAULT, labels, FilterTransformConfig.RELATIONSHIP_START_FIELD_DEFAULT, FilterTransformConfig.RELATIONSHIP_END_FIELD_DEFAULT);

        //define the FilterTransform
        FilterTransform<SourceRecord> filterValue = new FilterTransform.Value<SourceRecord>();
        filterValue.configure(config);

        //get config
        Assert.assertFalse(filterValue.config().equals(config));

        SourceRecord newRecord = filterValue.apply(new SourceRecord(null, null, "topic", null, null, record));
        filterValue.close();
        System.out.println(newRecord.toString());
        Assert.assertTrue(newRecord.value() != null);
    }

    @Test
    /**
     * Sends the node cdc record through because it contains the defined labels (in payload.after), field is an integer
     */
    public void passNodeRecordAfterInteger() throws IOException {
        //define value
        String json = "{\n" +
                "  \"meta\": {\n" +
                "    \"timestamp\": 1532597182604,\n" +
                "    \"username\": \"neo4j\",\n" +
                "    \"tx_id\": 3,\n" +
                "    \"tx_event_id\": 0,\n" +
                "    \"tx_events_count\": 2,\n" +
                "    \"operation\": \"created\",\n" +
                "    \"source\": {\n" +
                "      \"hostname\": \"neo4j.mycompany.com\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"payload\": {\n" +
                "    \"id\": \"1004\",\n" +
                "    \"type\": \"node\",\n" +
                "    \"after\": {\n" +
                "      \"labels\": 10,\n" +
                "      \"properties\": {\n" +
                "        \"email\": \"annek@noanswer.org\",\n" +
                "        \"last_name\": \"Kretchmar\",\n" +
                "        \"first_name\": \"Anne Marie\"\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"schema\": {\n" +
                "    \"properties\": {\n" +
                "      \"last_name\": \"String\",\n" +
                "      \"email\": \"String\",\n" +
                "      \"first_name\": \"String\"\n" +
                "    },\n" +
                "    \"constraints\": [{\n" +
                "      \"label\": \"Person\",\n" +
                "      \"properties\": [\"first_name\", \"last_name\"],\n" +
                "      \"type\": \"UNIQUE\"\n" +
                "    }]\n" +
                "  }\n" +
                "}";

        Map<String, Object> record = JsonConverter.jsonStringToMap(json);

        //define config
        List<String> labels = new ArrayList<>();
        labels.add("10");
        Map<String, Object> config = getConfig(FilterTransformConfig.NODE_FIELD_CONFIG_DEFAULT, FilterTransformConfig.NODE_SECONDARY_FIELD_CONFIG_DEFAULT, labels, FilterTransformConfig.RELATIONSHIP_START_FIELD_DEFAULT, FilterTransformConfig.RELATIONSHIP_END_FIELD_DEFAULT);

        //define the FilterTransform
        FilterTransform<SourceRecord> filterValue = new FilterTransform.Value<SourceRecord>();
        filterValue.configure(config);

        //get config
        Assert.assertFalse(filterValue.config().equals(config));

        SourceRecord newRecord = filterValue.apply(new SourceRecord(null, null, "topic", null, null, record));
        filterValue.close();
        System.out.println(newRecord.toString());
        Assert.assertTrue(newRecord.value() != null);
    }

    @Test
    /**
     * Sends the node cdc record through because it contains the defined labels (in payload.after), field is a long
     */
    public void passNodeRecordAfterLong() throws IOException {
        //define value
        String json = "{\n" +
                "  \"meta\": {\n" +
                "    \"timestamp\": 1532597182604,\n" +
                "    \"username\": \"neo4j\",\n" +
                "    \"tx_id\": 3,\n" +
                "    \"tx_event_id\": 0,\n" +
                "    \"tx_events_count\": 2,\n" +
                "    \"operation\": \"created\",\n" +
                "    \"source\": {\n" +
                "      \"hostname\": \"neo4j.mycompany.com\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"payload\": {\n" +
                "    \"id\": \"1004\",\n" +
                "    \"type\": \"node\",\n" +
                "    \"after\": {\n" +
                "      \"labels\": 1572361327791,\n" +
                "      \"properties\": {\n" +
                "        \"email\": \"annek@noanswer.org\",\n" +
                "        \"last_name\": \"Kretchmar\",\n" +
                "        \"first_name\": \"Anne Marie\"\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"schema\": {\n" +
                "    \"properties\": {\n" +
                "      \"last_name\": \"String\",\n" +
                "      \"email\": \"String\",\n" +
                "      \"first_name\": \"String\"\n" +
                "    },\n" +
                "    \"constraints\": [{\n" +
                "      \"label\": \"Person\",\n" +
                "      \"properties\": [\"first_name\", \"last_name\"],\n" +
                "      \"type\": \"UNIQUE\"\n" +
                "    }]\n" +
                "  }\n" +
                "}";

        Map<String, Object> record = JsonConverter.jsonStringToMap(json);

        //define config
        List<String> labels = new ArrayList<>();
        labels.add("1572361327791");
        Map<String, Object> config = getConfig(FilterTransformConfig.NODE_FIELD_CONFIG_DEFAULT, FilterTransformConfig.NODE_SECONDARY_FIELD_CONFIG_DEFAULT, labels, FilterTransformConfig.RELATIONSHIP_START_FIELD_DEFAULT, FilterTransformConfig.RELATIONSHIP_END_FIELD_DEFAULT);

        //define the FilterTransform
        FilterTransform<SourceRecord> filterValue = new FilterTransform.Value<SourceRecord>();
        filterValue.configure(config);

        //get config
        Assert.assertFalse(filterValue.config().equals(config));

        SourceRecord newRecord = filterValue.apply(new SourceRecord(null, null, "topic", null, null, record));
        filterValue.close();
        System.out.println(newRecord.toString());
        Assert.assertTrue(newRecord.value() != null);
    }

    @Test
    /**
     * Sends the node cdc record through because it contains the defined labels (in payload.after), field is a boolean
     */
    public void passNodeRecordAfterBoolean() throws IOException {
        //define value
        String json = "{\n" +
                "  \"meta\": {\n" +
                "    \"timestamp\": 1532597182604,\n" +
                "    \"username\": \"neo4j\",\n" +
                "    \"tx_id\": 3,\n" +
                "    \"tx_event_id\": 0,\n" +
                "    \"tx_events_count\": 2,\n" +
                "    \"operation\": \"created\",\n" +
                "    \"source\": {\n" +
                "      \"hostname\": \"neo4j.mycompany.com\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"payload\": {\n" +
                "    \"id\": \"1004\",\n" +
                "    \"type\": \"node\",\n" +
                "    \"after\": {\n" +
                "      \"labels\": true,\n" +
                "      \"properties\": {\n" +
                "        \"email\": \"annek@noanswer.org\",\n" +
                "        \"last_name\": \"Kretchmar\",\n" +
                "        \"first_name\": \"Anne Marie\"\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"schema\": {\n" +
                "    \"properties\": {\n" +
                "      \"last_name\": \"String\",\n" +
                "      \"email\": \"String\",\n" +
                "      \"first_name\": \"String\"\n" +
                "    },\n" +
                "    \"constraints\": [{\n" +
                "      \"label\": \"Person\",\n" +
                "      \"properties\": [\"first_name\", \"last_name\"],\n" +
                "      \"type\": \"UNIQUE\"\n" +
                "    }]\n" +
                "  }\n" +
                "}";

        Map<String, Object> record = JsonConverter.jsonStringToMap(json);

        //define config
        List<String> labels = new ArrayList<>();
        labels.add("true");
        Map<String, Object> config = getConfig(FilterTransformConfig.NODE_FIELD_CONFIG_DEFAULT, FilterTransformConfig.NODE_SECONDARY_FIELD_CONFIG_DEFAULT, labels, FilterTransformConfig.RELATIONSHIP_START_FIELD_DEFAULT, FilterTransformConfig.RELATIONSHIP_END_FIELD_DEFAULT);

        //define the FilterTransform
        FilterTransform<SourceRecord> filterValue = new FilterTransform.Value<SourceRecord>();
        filterValue.configure(config);

        //get config
        Assert.assertFalse(filterValue.config().equals(config));

        SourceRecord newRecord = filterValue.apply(new SourceRecord(null, null, "topic", null, null, record));
        filterValue.close();
        System.out.println(newRecord.toString());
        Assert.assertTrue(newRecord.value() != null);
    }

    @Test
    /**
     * Sends the relationship cdc record through because it contains the defined labels
     */
    public void passRelationshipRecord() throws IOException {
        //define value
        String json = "{\n" +
                "  \"meta\": {\n" +
                "    \"timestamp\": 1532597182604,\n" +
                "    \"username\": \"neo4j\",\n" +
                "    \"tx_id\": 3,\n" +
                "    \"tx_event_id\": 0,\n" +
                "    \"tx_events_count\": 2,\n" +
                "    \"operation\": \"created\",\n" +
                "    \"source\": {\n" +
                "      \"hostname\": \"neo4j.mycompany.com\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"payload\": {\n" +
                "    \"id\": \"123\",\n" +
                "    \"type\": \"relationship\",\n" +
                "    \"label\": \"KNOWS\",\n" +
                "    \"start\": {\n" +
                "      \"labels\": [\"Person\"],\n" +
                "      \"id\": \"123\",\n" +
                "      \"ids\": {\n" +
                "        \"last_name\": \"Andrea\",\n" +
                "        \"first_name\": \"Santurbano\"\n" +
                "      }\n" +
                "    },\n" +
                "    \"end\": {\n" +
                "      \"labels\": [\"Person\"],\n" +
                "      \"id\": \"456\",\n" +
                "      \"ids\": {\n" +
                "        \"last_name\": \"Michael\",\n" +
                "        \"first_name\": \"Hunger\"\n" +
                "      }\n" +
                "    },\n" +
                "    \"after\": {\n" +
                "      \"properties\": {\n" +
                "        \"since\": \"2018-04-05T12:34:00[Europe/Berlin]\"\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"schema\": {\n" +
                "    \"properties\": {\n" +
                "      \"since\": \"ZonedDateTime\"\n" +
                "    },\n" +
                "    \"constraints\": [{\n" +
                "      \"label\": \"KNOWS\",\n" +
                "      \"properties\": [\"since\"],\n" +
                "      \"type\": \"RELATIONSHIP_PROPERTY_EXISTS\"\n" +
                "    }]\n" +
                "  }\n" +
                "}";

        Map<String, Object> record = JsonConverter.jsonStringToMap(json);

        //define config
        List<String> labels = new ArrayList<>();
        labels.add("Person");
        Map<String, Object> config = getConfig(FilterTransformConfig.NODE_FIELD_CONFIG_DEFAULT, FilterTransformConfig.NODE_SECONDARY_FIELD_CONFIG_DEFAULT, labels, FilterTransformConfig.RELATIONSHIP_START_FIELD_DEFAULT, FilterTransformConfig.RELATIONSHIP_END_FIELD_DEFAULT);

        //define the FilterTransform
        FilterTransform<SourceRecord> filterValue = new FilterTransform.Value<SourceRecord>();
        filterValue.configure(config);

        //get config
        Assert.assertFalse(filterValue.config().equals(config));

        SourceRecord newRecord = filterValue.apply(new SourceRecord(null, null, "topic", null, null, record));
        filterValue.close();
        System.out.println(newRecord.toString());
        Assert.assertTrue(newRecord.value() != null);
    }

    @Test
    /**
     * Filters the relationship cdc record because it does not contain the defined labels (in one of the end or start nodes)
     */
    public void filterRelationshipRecord() throws IOException {
        //define value
        String json = "{\n" +
                "  \"meta\": {\n" +
                "    \"timestamp\": 1532597182604,\n" +
                "    \"username\": \"neo4j\",\n" +
                "    \"tx_id\": 3,\n" +
                "    \"tx_event_id\": 0,\n" +
                "    \"tx_events_count\": 2,\n" +
                "    \"operation\": \"created\",\n" +
                "    \"source\": {\n" +
                "      \"hostname\": \"neo4j.mycompany.com\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"payload\": {\n" +
                "    \"id\": \"123\",\n" +
                "    \"type\": \"relationship\",\n" +
                "    \"label\": \"KNOWS\",\n" +
                "    \"start\": {\n" +
                "      \"labels\": [\"Person\"],\n" +
                "      \"id\": \"123\",\n" +
                "      \"ids\": {\n" +
                "        \"last_name\": \"Andrea\",\n" +
                "        \"first_name\": \"Santurbano\"\n" +
                "      }\n" +
                "    },\n" +
                "    \"end\": {\n" +
                "      \"labels\": [\"Not Person\"],\n" +
                "      \"id\": \"456\",\n" +
                "      \"ids\": {\n" +
                "        \"last_name\": \"Michael\",\n" +
                "        \"first_name\": \"Hunger\"\n" +
                "      }\n" +
                "    },\n" +
                "    \"after\": {\n" +
                "      \"properties\": {\n" +
                "        \"since\": \"2018-04-05T12:34:00[Europe/Berlin]\"\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"schema\": {\n" +
                "    \"properties\": {\n" +
                "      \"since\": \"ZonedDateTime\"\n" +
                "    },\n" +
                "    \"constraints\": [{\n" +
                "      \"label\": \"KNOWS\",\n" +
                "      \"properties\": [\"since\"],\n" +
                "      \"type\": \"RELATIONSHIP_PROPERTY_EXISTS\"\n" +
                "    }]\n" +
                "  }\n" +
                "}";

        Map<String, Object> record = JsonConverter.jsonStringToMap(json);

        //define config
        List<String> labels = new ArrayList<>();
        labels.add("Person");
        Map<String, Object> config = getConfig(FilterTransformConfig.NODE_FIELD_CONFIG_DEFAULT, FilterTransformConfig.NODE_SECONDARY_FIELD_CONFIG_DEFAULT, labels, FilterTransformConfig.RELATIONSHIP_START_FIELD_DEFAULT, FilterTransformConfig.RELATIONSHIP_END_FIELD_DEFAULT);

        //define the FilterTransform
        FilterTransform<SourceRecord> filterValue = new FilterTransform.Value<SourceRecord>();
        filterValue.configure(config);

        //get config
        Assert.assertFalse(filterValue.config().equals(config));

        SourceRecord newRecord = filterValue.apply(new SourceRecord(null, null, "topic", null, null, record));
        filterValue.close();
        System.out.println(newRecord.toString());
        Assert.assertTrue(newRecord.value() == null);
    }

    @Test
    /**
     * Throws a data exception because the record is not a map type
     */
    public void recordNotAMap() {
        //define value
        List<String> falseArray = new ArrayList<>();
        falseArray.add("a");
        falseArray.add("b");
        falseArray.add("c");


        //define config
        List<String> labels = new ArrayList<>();
        labels.add("Person");
        Map<String, Object> config = getConfig(FilterTransformConfig.NODE_FIELD_CONFIG_DEFAULT, FilterTransformConfig.NODE_SECONDARY_FIELD_CONFIG_DEFAULT, labels, FilterTransformConfig.RELATIONSHIP_START_FIELD_DEFAULT, FilterTransformConfig.RELATIONSHIP_END_FIELD_DEFAULT);

        //define the FilterTransform
        FilterTransform<SourceRecord> filterValue = new FilterTransform.Value<SourceRecord>();
        filterValue.configure(config);

        //get config
        Assert.assertFalse(filterValue.config().equals(config));

        try {
            SourceRecord newRecord = filterValue.apply(new SourceRecord(null, null, "topic", null, null, falseArray));
        } catch (DataException e) {
        }
    }

    @Test
    /**
     * Throws an exception because the cdc type is not supported (it is n ot a node cdc or relationship cdc)
     */
    public void cdcTypeNotSupported() throws IOException {
        //define value
        String json = "{\n" +
                "  \"meta\": {\n" +
                "    \"timestamp\": 1532597182604,\n" +
                "    \"username\": \"neo4j\",\n" +
                "    \"tx_id\": 3,\n" +
                "    \"tx_event_id\": 0,\n" +
                "    \"tx_events_count\": 2,\n" +
                "    \"operation\": \"created\",\n" +
                "    \"source\": {\n" +
                "      \"hostname\": \"neo4j.mycompany.com\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"payload\": {\n" +
                "    \"id\": \"123\",\n" +
                "    \"type\": \"not supported\",\n" +
                "    \"label\": \"KNOWS\",\n" +
                "    \"start\": {\n" +
                "      \"labels\": [\"Person\"],\n" +
                "      \"id\": \"123\",\n" +
                "      \"ids\": {\n" +
                "        \"last_name\": \"Andrea\",\n" +
                "        \"first_name\": \"Santurbano\"\n" +
                "      }\n" +
                "    },\n" +
                "    \"end\": {\n" +
                "      \"labels\": [\"Not Person\"],\n" +
                "      \"id\": \"456\",\n" +
                "      \"ids\": {\n" +
                "        \"last_name\": \"Michael\",\n" +
                "        \"first_name\": \"Hunger\"\n" +
                "      }\n" +
                "    },\n" +
                "    \"after\": {\n" +
                "      \"properties\": {\n" +
                "        \"since\": \"2018-04-05T12:34:00[Europe/Berlin]\"\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"schema\": {\n" +
                "    \"properties\": {\n" +
                "      \"since\": \"ZonedDateTime\"\n" +
                "    },\n" +
                "    \"constraints\": [{\n" +
                "      \"label\": \"KNOWS\",\n" +
                "      \"properties\": [\"since\"],\n" +
                "      \"type\": \"RELATIONSHIP_PROPERTY_EXISTS\"\n" +
                "    }]\n" +
                "  }\n" +
                "}";

        Map<String, Object> record = JsonConverter.jsonStringToMap(json);

        //define config
        List<String> labels = new ArrayList<>();
        labels.add("Person");
        Map<String, Object> config = getConfig(FilterTransformConfig.NODE_FIELD_CONFIG_DEFAULT, FilterTransformConfig.NODE_SECONDARY_FIELD_CONFIG_DEFAULT, labels, FilterTransformConfig.RELATIONSHIP_START_FIELD_DEFAULT, FilterTransformConfig.RELATIONSHIP_END_FIELD_DEFAULT);

        //define the FilterTransform
        FilterTransform<SourceRecord> filterValue = new FilterTransform.Value<SourceRecord>();
        filterValue.configure(config);

        //get config
        Assert.assertFalse(filterValue.config().equals(config));
        try {
            SourceRecord newRecord = filterValue.apply(new SourceRecord(null, null, "topic", null, null, record));
        } catch (IllegalArgumentException e) {

        }
    }


}


