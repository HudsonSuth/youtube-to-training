package abac.mask;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Below clearance 3, masks high-sensitivity fields and any field the snapshot
 * does not list as low. An empty or missing snapshot therefore publishes no raw values.
 */
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
        Map<String, String> fields = sensitivityByField == null ? Map.of() : sensitivityByField;
        List<String> names = new ArrayList<>();
        object.fieldNames().forEachRemaining(names::add);
        for (String name : names) {
            if (!"low".equals(fields.get(name))) {
                object.put(name, MASK);
            }
        }
        try {
            return MAPPER.writeValueAsString(object);
        } catch (Exception ex) {
            return MASK;
        }
    }
}
