package si.hekovnik.transform;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.List;
import java.util.Map;

public class FilterTransformConfig extends AbstractConfig {

    //Define which labels have permission (are entitled)
    public static final String LABELS = "labels";

    //Define configuration to handle node CDC
    public static final String NODE_FIELD_CONFIG = "node.field";
    public static final String NODE_FIELD_CONFIG_DEFAULT = "payload.after.labels";
    public static final String NODE_SECONDARY_FIELD_CONFIG = "node.second.field";
    public static final String NODE_SECONDARY_FIELD_CONFIG_DEFAULT = "payload.before.labels";

    //Define configuration to handle relationship CDC
    public static final String RELATIONSHIP_START_FIELD = "relationship.start.field";
    public static final String RELATIONSHIP_START_FIELD_DEFAULT = "payload.start.labels";
    public static final String RELATIONSHIP_END_FIELD = "relationship.end.field";
    public static final String RELATIONSHIP_END_FIELD_DEFAULT = "payload.end.labels";

    //Payload type
    private static final String PAYLOAD_TYPE_FIELD = "payload.type";
    private static final String PAYLOAD_NODE_TYPE = "node";
    private static final String PAYLOAD_RELATIONSHIP_TYPE ="relationship";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            //configuration for node CDC
            .define(NODE_FIELD_CONFIG,
                    ConfigDef.Type.STRING,
                    NODE_FIELD_CONFIG_DEFAULT,
                    ConfigDef.Importance.LOW,
                    "The node field containing the labels on which to check the equality to perform message filtering.")
            .define(LABELS,
                    ConfigDef.Type.LIST,
                    ConfigDef.Importance.HIGH,
                    "The labels to compare to the node labels.")
            .define(NODE_SECONDARY_FIELD_CONFIG,
                    ConfigDef.Type.STRING,
                    NODE_SECONDARY_FIELD_CONFIG_DEFAULT,
                    ConfigDef.Importance.LOW,
                    "The secondary node field containing the labels on which to check the equality to perform message filtering if the primary node field is empty.")
            //configuration for relationship CDC
            .define(RELATIONSHIP_START_FIELD,
                    ConfigDef.Type.STRING,
                    RELATIONSHIP_START_FIELD_DEFAULT,
                    ConfigDef.Importance.LOW,
                    "The name of the relationship field that represents the start node.")
            .define(RELATIONSHIP_END_FIELD,
                    ConfigDef.Type.STRING,
                    RELATIONSHIP_END_FIELD_DEFAULT,
                    ConfigDef.Importance.LOW,
                    "The name of the relationship field that represents the end node.");



    private final String nodeField;
    private final List<String> labels;
    private final String secondNodeField;

    private final String relationStartField;
    private final String relationEndField;


    public FilterTransformConfig(final Map<?, ?> parsedConfig) {
        super(config(), parsedConfig);
        this.nodeField = getString(NODE_FIELD_CONFIG);
        this.labels = getList(LABELS);
        this.secondNodeField = getString(NODE_SECONDARY_FIELD_CONFIG);

        this.relationStartField = getString(RELATIONSHIP_START_FIELD);
        this.relationEndField = getString(RELATIONSHIP_END_FIELD);

        if (labels.isEmpty() || labels == null){
            throw new IllegalArgumentException("The labels must be defined.");
        }


    }

    public static ConfigDef config() {
        return CONFIG_DEF;
    }

    public String getNodeField() {
        return nodeField;
    }

    public List<String> getLabels() {
        return labels;
    }

    public String getSecondNodeField() {
        return secondNodeField;
    }

    public String getRelationStartField() {
        return relationStartField;
    }

    public String getRelationEndField() {
        return relationEndField;
    }

    public static String getPayloadNodeType() {
        return PAYLOAD_NODE_TYPE;
    }

    public static String getPayloadRelationshipType() {
        return PAYLOAD_RELATIONSHIP_TYPE;
    }

    public static String getPayloadTypeField() {
        return PAYLOAD_TYPE_FIELD;
    }
}