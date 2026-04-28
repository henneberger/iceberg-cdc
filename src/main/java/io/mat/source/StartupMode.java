package io.mat.source;

import java.io.Serializable;
import java.util.Locale;

public enum StartupMode implements Serializable {
    /** Read all currently visible snapshots, then continue streaming. */
    EARLIEST,
    /** Start after the table's current snapshot and consume only future snapshots. */
    LATEST;

    public static StartupMode fromOption(String value) {
        String normalized = value == null ? "earliest" : value.trim().toLowerCase(Locale.ROOT);
        switch (normalized) {
            case "earliest":
            case "initial":
                return EARLIEST;
            case "latest":
                return LATEST;
            default:
                throw new IllegalArgumentException(
                        "Unsupported scan.startup.mode '" + value
                                + "'. Expected 'earliest' or 'latest'.");
        }
    }
}
