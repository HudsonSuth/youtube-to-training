package abac.mask;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Map;

/** Masks high-sensitivity fields when the principal clearance is below 3. */
public final class FieldMasker {
    public static final String MASK = "***MASKED***";
    static final int UNKNOWN_CLEARANCE = Integer.MIN_VALUE;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private FieldMasker() {}

    public static String mask(String payload, Map<String, String> sensitivityByField, int clearance) {
        if (clearance >= 3) {
            return payload;
        }
        if (payload == null) {
            return MASK;
        }
        final JsonNode node;
        try {
            node = MAPPER.readTree(payload);
        } catch (Exception ex) {
            return MASK;
        }
        if (!(node instanceof ObjectNode object)) {
            return MASK;
        }
        for (Map.Entry<String, String> field : sensitivityByField.entrySet()) {
            if ("high".equals(field.getValue()) && object.has(field.getKey())) {
                object.put(field.getKey(), MASK);
            }
        }
        try {
            return MAPPER.writeValueAsString(object);
        } catch (Exception ex) {
            return MASK;
        }
    }
}
