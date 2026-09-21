package didatrade.util;

public enum DebugMode {
    CRASH(1),
    FREEZE(2),
    UNFREEZE(3),
    SLOW_ON(4),
    SLOW_OFF(5);

    private final int code;

    DebugMode(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static DebugMode fromCode(int code) {
        for (DebugMode mode : DebugMode.values()) {
            if (mode.getCode() == code) {
                return mode;
            }
        }
        throw new IllegalArgumentException("Invalid DebugMode code: " + code);
    }
}
