package eu.openanalytics.phaedra.calculationservice.enumeration.external;

public enum ApprovalStatus {
    APPROVAL_NOT_SET(0),
    APPROVAL_NOT_NEEDED(1),
    APPROVED(2),
    DISAPPROVED(-1),
    ;

    private int code;

    ApprovalStatus(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static ApprovalStatus getByCode(int code) {
        for (ApprovalStatus s: ApprovalStatus.values()) {
            if (s.getCode() == code) return s;
        }
        return null;
    }

//    public boolean matches(Plate plate) {
//        return getCode() == plate.getApprovalStatus();
//    }
}
