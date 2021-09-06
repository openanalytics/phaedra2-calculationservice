package eu.openanalytics.phaedra.calculationservice.enumeration.external;

public enum CalculationStatus {
    CALCULATION_NEEDED(0),
    CALCULATION_OK(1),
    CALCULATION_NOT_POSSIBLE(-1),
    CALCULATION_ERROR(-2),
    ;

    private int code;

    CalculationStatus(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static CalculationStatus getByCode(int code) {
        for (CalculationStatus s: CalculationStatus.values()) {
            if (s.getCode() == code) return s;
        }
        return null;
    }

//    public boolean matches(Plate plate) {
//        return getCode() == plate.getCalculationStatus();
//    }
}
