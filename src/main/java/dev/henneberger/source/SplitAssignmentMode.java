package dev.henneberger.source;

import java.io.Serializable;
import java.util.Locale;

public enum SplitAssignmentMode implements Serializable {
    /** Preserve global CDC order by routing all splits through one source subtask. */
    ORDERED,
    /** Higher throughput, but only preserves ordering for rows that stay in one data file. */
    FILE_AFFINITY;

    public static SplitAssignmentMode fromOption(String value) {
        String normalized = value == null ? "ordered" : value.trim().toLowerCase(Locale.ROOT);
        switch (normalized) {
            case "ordered":
                return ORDERED;
            case "file-affinity":
            case "file_affinity":
                return FILE_AFFINITY;
            default:
                throw new IllegalArgumentException(
                        "Unsupported split-assignment-mode '" + value
                                + "'. Expected 'ordered' or 'file-affinity'.");
        }
    }
}
