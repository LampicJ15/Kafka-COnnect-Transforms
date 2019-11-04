package si.hekovnik.transform;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import si.hekovnik.utils.MapUtils;

import java.util.List;
import java.util.Map;

public abstract class FilterTransform<R extends ConnectRecord<R>> implements Transformation<R> {

    private FilterTransformConfig config;

    public R apply(final R record) {
        // find a field in R based on the name (like in Timestamp transform)
        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(final R record) {
        final Map<String, Object> value = convertToMap(operatingValue(record));

        //if the defined field contains the specified value we return the record
        Object fieldValue;

        try {
            fieldValue = MapUtils.getField(value, config.getField(), "\\.");
        } catch (NullPointerException e) {
            fieldValue = MapUtils.getField(value, config.getSecondField(), "\\.");
        }
        ;

        if (fieldValue == null) {
            throw new IllegalArgumentException("There is no field or value for the given field name: " + config.getField() + " " + config.getSecondField());
        } else if (fieldValue instanceof List) {
            if (((List) fieldValue).contains(config.getFieldValue())) {
                return newRecord(record, value);
            }
        } else if (fieldValue instanceof String) {
            if (fieldValue.equals(config.getFieldValue())) {
                return newRecord(record, value);
            }
        } else if (fieldValue instanceof Integer) {
            if (fieldValue.toString().equals(config.getFieldValue())) {
                return newRecord(record, value);
            }
        } else if (fieldValue instanceof Long) {
            if ((fieldValue.toString().replace("L", "")).equals(config.getFieldValue())) {
                return newRecord(record, value);
            }
        }

        //otherwise we return null
        return newRecord(record, null);
    }

    private R applyWithSchema(final R record) {
        final Schema schema = operatingSchema(record);
        final Struct value = convertToStruct(operatingValue(record));

        final Struct updatedValue = new Struct(value.schema());
        for (Field field : schema.fields()) {
            //check if the defined field contains the specified value
            if (field.name().equals(config.getField())) {
                //if it does not contain the value, we skip the record
                if (!(value.get(field).equals(config.getFieldValue()))) {
                    return newRecord(record, null);
                }
            }
        }
        //we return the default record
        return record;
    }


    public ConfigDef config() {
        return FilterTransformConfig.CONFIG_DEF;
    }

    public void close() {
    }

    public void configure(final Map<String, ?> parsedConfig) {
        this.config = new FilterTransformConfig(parsedConfig);
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(final R record);

    protected abstract R newRecord(R record, Object updatedValue);

    private Map<String, Object> convertToMap(Object value) {
        if (!(value instanceof Map)) {
            throw new DataException("Only Map objects supported in absence of schema for regex transformation, found: " + getClassName(value));
        }
        return (Map<String, Object>) value;
    }

    private Struct convertToStruct(Object value) {
        if (!(value instanceof Struct)) {
            throw new DataException("Only Struct objects supported for regex transformation, found: " + getClassName(value));
        }
        return (Struct) value;
    }

    private String getClassName(final Object value) {
        return value == null ? "null" : value.getClass().getName();
    }

    public static class Key<R extends ConnectRecord<R>> extends FilterTransform<R> {

        @Override
        protected Schema operatingSchema(final R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(final R record) {
            return record.key();
        }

        @Override
        protected R newRecord(final R record, final Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends FilterTransform<R> {

        @Override
        protected Schema operatingSchema(final R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(final R record) {
            return record.value();
        }

        @Override
        protected R newRecord(final R record, final Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), updatedValue, record.timestamp());
        }
    }
}