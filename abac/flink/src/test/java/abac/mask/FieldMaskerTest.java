package abac.mask;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.junit.jupiter.api.Test;

class FieldMaskerTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Map<String, String> FIELDS = Map.of("ssn", "high", "amount", "low");

    @Test
    void masksHighFieldsWhenClearanceIsBelow3() throws Exception {
        String masked = FieldMasker.mask("{\"ssn\":\"123-45-6789\",\"amount\":4200}", FIELDS, 1);
        JsonNode node = MAPPER.readTree(masked);
        assertEquals(FieldMasker.MASK, node.get("ssn").asText());
        assertEquals(4200, node.get("amount").asInt());
        assertFalse(masked.contains("123-45-6789"));
    }

    @Test
    void leavesRecordsAloneAtClearance3() {
        String raw = "{\"ssn\":\"123-45-6789\",\"amount\":4200}";
        assertEquals(raw, FieldMasker.mask(raw, FIELDS, 3));
        assertEquals(raw, FieldMasker.mask(raw, Map.of(), 3));
    }

    @Test
    void emptyFieldSnapshotDoesNotPublishRawValues() throws Exception {
        String masked = FieldMasker.mask("{\"ssn\":\"123-45-6789\",\"amount\":4200}", Map.of(), 1);
        JsonNode node = MAPPER.readTree(masked);
        assertEquals(FieldMasker.MASK, node.get("ssn").asText());
        assertEquals(FieldMasker.MASK, node.get("amount").asText());
        assertFalse(masked.contains("123-45-6789"));
        assertFalse(masked.contains("4200"));
        assertFalse(FieldMasker.mask("{\"ssn\":\"123-45-6789\"}", null, 1).contains("123-45-6789"));
    }

    @Test
    void unknownFieldsAreMaskedBelowClearance3() throws Exception {
        String masked = FieldMasker.mask("{\"ssn\":\"123-45-6789\",\"amount\":4200}", Map.of("amount", "low"), 1);
        JsonNode node = MAPPER.readTree(masked);
        assertEquals(FieldMasker.MASK, node.get("ssn").asText());
        assertEquals(4200, node.get("amount").asInt());
    }

    @Test
    void unknownClearanceFailsClosed() {
        String masked = FieldMasker.mask("{\"ssn\":\"123-45-6789\"}", FIELDS, FieldMasker.UNKNOWN_CLEARANCE);
        assertTrue(masked.contains(FieldMasker.MASK));
        assertFalse(masked.contains("123-45-6789"));
    }

    @Test
    void unreadablePayloadIsMaskedForLowClearance() {
        assertEquals(FieldMasker.MASK, FieldMasker.mask("not-json", FIELDS, 1));
        assertEquals("not-json", FieldMasker.mask("not-json", FIELDS, 3));
    }
}
