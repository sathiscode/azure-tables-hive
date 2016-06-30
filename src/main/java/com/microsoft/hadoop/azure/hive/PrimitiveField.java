package com.microsoft.hadoop.azure.hive;

import java.sql.Timestamp;

import org.apache.hadoop.hive.serde2.objectinspector.*;

import com.google.common.base.Joiner;
import com.microsoft.azure.storage.table.*;

/**
 * A field in an Azure Tables entity.
 */
class PrimitiveField implements StructField {

    private final int fieldID;
    private final EntityPropertyInspector inspector;
    private final String name;
    private final boolean requireFieldExists;

    public PrimitiveField(int fieldID, String name, EntityPropertyInspector inspector, boolean requireFieldExists) {
        this.fieldID = fieldID;
        this.name = name;
        this.inspector = inspector;
        this.requireFieldExists = requireFieldExists;
    }

    @Override
    public int getFieldID() {
        return fieldID;
    }

    @Override
    public String getFieldComment() {
        return null;
    }

    @Override
    public String getFieldName() {
        return name;
    }

    @Override
    public ObjectInspector getFieldObjectInspector() {
        return inspector;
    }

    public Object getData(DynamicTableEntity entity) {
        Object value;
        if (name.equalsIgnoreCase("RowKey")) {
            value = entity.getRowKey();
        } else if (name.equalsIgnoreCase("PartitionKey")) {
            value = entity.getPartitionKey();
        } else if (name.equalsIgnoreCase("Timestamp")) {
            value = new Timestamp(entity.getTimestamp().getTime());
        } else {
            value = entity.getProperties().get(name);
            if (value == null) {
                // Look for it ignoring case (Hive normalizes to lower case).
                for (String key : entity.getProperties().keySet()) {
                    if (key.equalsIgnoreCase(name)) {
                        value = entity.getProperties().get(key);
                        break;
                    }
                }
                if (value == null) {
                    if (requireFieldExists) {
                        throw new IllegalArgumentException(
                                "No property found with name " + name
                                + ". Properties found: "
                                + Joiner.on(',').join(entity.getProperties().keySet()));
                    } else {
                        return null;
                    }
                }
            }
        }
        return inspector.getPrimitiveJavaObject(value);
    }
}
