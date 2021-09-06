package eu.openanalytics.phaedra.calculationservice.enumeration.external;

public enum UploadStatus {
    UPLOAD_NOT_SET(0),
    UPLOAD_NOT_NEEDED(1),
    UPLOADED(2),
    ;

    private int code;

    UploadStatus(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static UploadStatus getByCode(int code) {
        for (UploadStatus s: UploadStatus.values()) {
            if (s.getCode() == code) return s;
        }
        return null;
    }

//    public boolean matches(Plate plate) {
//        return getCode() == plate.getUploadStatus();
//    }
}
