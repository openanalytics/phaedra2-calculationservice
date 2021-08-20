package eu.openanalytics.phaedra.calculationservice.scriptengineclient.config;


import eu.openanalytics.phaedra.calculationservice.scriptengineclient.model.TargetRuntime;

import java.util.HashMap;
import java.util.Map;

// Must be configured as bean
public class ScriptEngineClientConfiguration {

    private final Map<String, TargetRuntime> targetRuntimes = new HashMap<>();

    private String clientName;

    public String getClientName() {
        return clientName;
    }

    public String getResponseQueueName() {
        return "scriptengine_output_for_" + clientName;
    }

    public void setClientName(String clientName) {
        this.clientName = clientName;
    }

    public void addTargetRuntime(String name, TargetRuntime targetRuntime) {
        targetRuntimes.put(name, targetRuntime);
    }

    public TargetRuntime getTargetRuntime(String targetName) {
        if (!targetRuntimes.containsKey(targetName)) {
            throw new IllegalArgumentException("Target not found with name " + targetName);
        }
        return targetRuntimes.get(targetName);
    }
}
