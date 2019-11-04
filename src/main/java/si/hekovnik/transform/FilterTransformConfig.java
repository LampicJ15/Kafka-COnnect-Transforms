package si.hekovnik.transform;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.Map;

public class FilterTransformConfig extends AbstractConfig {

    public static final String FIELD_CONFIG = "field";

    public static final String FIELD_VALUE = "field.value";

    public static final String SECONDARY_FIELD_CONFIG = "second.field";
    private static final String FIELD_DEFAULT = "";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELD_CONFIG,
                    ConfigDef.Type.STRING,
                    ConfigDef.Importance.HIGH,
                    "The field containing the value on which to check the equality to perform message filtering.")
            .define(FIELD_VALUE,
                    ConfigDef.Type.STRING,
                    ConfigDef.Importance.HIGH,
                    "The value to compare to the field value.")
            .define(SECONDARY_FIELD_CONFIG,
                    ConfigDef.Type.STRING,
                    FIELD_DEFAULT,
                    ConfigDef.Importance.LOW,
                    "The secondary field containing the value on which to check the equality to perform message filtering if the primary field is empty.");


    private final String field;
    private final String fieldValue;
    private final String secondField;

    public FilterTransformConfig(final Map<?, ?> parsedConfig) {
        super(config(), parsedConfig);
        this.field = getString(FIELD_CONFIG);
        this.fieldValue = getString(FIELD_VALUE);
        this.secondField = getString(SECONDARY_FIELD_CONFIG);

        if (field.isEmpty() || fieldValue.isEmpty() || field == null || fieldValue == null) {
            throw new ConfigException("Field and field value must be specified.");
        }
    }


    public static ConfigDef config() {
        return CONFIG_DEF;
    }

    public String getField() {
        return field;
    }

    public String getFieldValue() {
        return fieldValue;
    }

    public String getSecondField() {
        return secondField;
    }
}