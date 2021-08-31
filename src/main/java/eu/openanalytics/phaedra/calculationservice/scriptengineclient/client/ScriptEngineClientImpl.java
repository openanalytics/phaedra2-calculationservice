package eu.openanalytics.phaedra.calculationservice.scriptengineclient.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.openanalytics.phaedra.calculationservice.scriptengineclient.config.ScriptEngineClientConfiguration;
import eu.openanalytics.phaedra.calculationservice.scriptengineclient.model.ScriptExecutionInput;
import eu.openanalytics.phaedra.calculationservice.scriptengineclient.model.ScriptExecutionOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class ScriptEngineClientImpl implements MessageListener, ScriptEngineClient {

    private final ScriptEngineClientConfiguration clientConfig;
    private final RabbitTemplate rabbitTemplate;
    private final ObjectMapper objectMapper;

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final ConcurrentHashMap<String, ScriptExecutionInput> executionsInProgress = new ConcurrentHashMap<>();

    public ScriptEngineClientImpl(ScriptEngineClientConfiguration clientConfig, RabbitTemplate rabbitTemplate) {
        this.clientConfig = clientConfig;
        this.rabbitTemplate = rabbitTemplate;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public ScriptExecutionInput newScriptExecution(String targetName, String script, String input) {
        var target = clientConfig.getTargetRuntime(targetName);

        return new ScriptExecutionInput(
                target,
                script,
                input,
                clientConfig.getClientName());
    }

    @Override
    public void execute(ScriptExecutionInput input) throws JsonProcessingException {
        // send message
        rabbitTemplate.send(
                "scriptengine_input",
                input.getTargetRuntime().getRoutingKey(),
                new Message(objectMapper.writeValueAsBytes(new HashMap<>() {{
                    put("id", input.getId());
                    put("input", input.getInput());
                    put("script", input.getScript());
                    put("response_topic_suffix", input.getResponseTopicSuffix());
                    put("queue_timestamp", System.currentTimeMillis());
                }})));

        executionsInProgress.put(input.getId().toString(), input);
    }

    @Override
    public void onMessage(Message message) {
        try {
            ScriptExecutionOutput output = objectMapper.readValue(message.getBody(), ScriptExecutionOutput.class);

            var input = executionsInProgress.get(output.getInputId());
            if (input != null) {
                input.getOutput().complete(output);
            } else {
                logger.warn("No execution found, for output id " + output.getInputId());
            }
        } catch (IOException e) {
            logger.warn("Exception during handling of incoming output message", e);
        }
        // TODO delete future
    }

}
