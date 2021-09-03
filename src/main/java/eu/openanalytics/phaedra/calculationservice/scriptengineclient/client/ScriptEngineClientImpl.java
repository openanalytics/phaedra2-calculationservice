package eu.openanalytics.phaedra.calculationservice.scriptengineclient.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.openanalytics.phaedra.calculationservice.scriptengineclient.config.ScriptEngineClientConfiguration;
import eu.openanalytics.phaedra.calculationservice.scriptengineclient.model.ScriptExecution;
import eu.openanalytics.phaedra.model.v2.dto.ScriptExecutionOutputDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public class ScriptEngineClientImpl implements MessageListener, ScriptEngineClient {

    private final ScriptEngineClientConfiguration clientConfig;
    private final RabbitTemplate rabbitTemplate;
    private final ObjectMapper objectMapper;

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final ConcurrentHashMap<String, ScriptExecution> executionsInProgress = new ConcurrentHashMap<>();

    public ScriptEngineClientImpl(ScriptEngineClientConfiguration clientConfig, RabbitTemplate rabbitTemplate) {
        this.clientConfig = clientConfig;
        this.rabbitTemplate = rabbitTemplate;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public ScriptExecution newScriptExecution(String targetName, String script, String input) {
        var target = clientConfig.getTargetRuntime(targetName);

        return new ScriptExecution(
                target,
                script,
                input,
                clientConfig.getClientName()
        );
    }

    @Override
    public void execute(ScriptExecution scripExecution) throws JsonProcessingException {
        // send message
        rabbitTemplate.send(
                "scriptengine_input",
                scripExecution.getTargetRuntime().getRoutingKey(),
                new Message(objectMapper.writeValueAsBytes(scripExecution.getScriptExecutionInput())));

        executionsInProgress.put(scripExecution.getScriptExecutionInput().getId(), scripExecution);
    }

    @Override
    public void onMessage(Message message) {
        try {
            ScriptExecutionOutputDTO output = objectMapper.readValue(message.getBody(), ScriptExecutionOutputDTO.class);

            var scriptExecution = executionsInProgress.get(output.getInputId());
            if (scriptExecution != null) {
                scriptExecution.getOutput().complete(output);
            } else {
                logger.warn("No execution found, for output id " + output.getInputId());
            }
        } catch (IOException e) {
            logger.warn("Exception during handling of incoming output message", e);
        }
        // TODO delete future
    }

}
