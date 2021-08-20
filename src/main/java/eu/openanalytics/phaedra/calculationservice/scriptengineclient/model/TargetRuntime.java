package eu.openanalytics.phaedra.calculationservice.scriptengineclient.model;

public class TargetRuntime {

    private final String language;
    private final String poolName;
    private final String version;

    public TargetRuntime(String language, String poolName, String version) {
        this.language = language;
        this.poolName = poolName;
        this.version = version;
    }

    public String getLanguage() {
        return language;
    }

    public String getPoolName() {
        return poolName;
    }

    public String getVersion() {
        return version;
    }

    public String getRoutingKey() {
        return String.format("scriptengine.input.%s.%s.%s", poolName, language, version);
    }
}
