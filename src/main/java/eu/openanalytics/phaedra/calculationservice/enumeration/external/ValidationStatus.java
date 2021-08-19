package eu.openanalytics.phaedra.calculationservice.enumeration.external;

public enum ValidationStatus {
    VALIDATION_NOT_SET(0),
    VALIDATION_NOT_NEEDED(1),
    VALIDATED(2),
    INVALIDATED(-1),
    ;

    private int code;

    ValidationStatus(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static ValidationStatus getByCode(int code) {
        for (ValidationStatus s: ValidationStatus.values()) {
            if (s.getCode() == code) return s;
        }
        return null;
    }

//    public boolean matches(Plate plate) {
//        return getCode() == plate.getValidationStatus();
//    }
}
