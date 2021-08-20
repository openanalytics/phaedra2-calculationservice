package eu.openanalytics.phaedra.calculationservice.scriptengineclient.config;

import eu.openanalytics.phaedra.calculationservice.scriptengineclient.client.ScriptEngineClient;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.DirectMessageListenerContainer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Configuration
public class ScriptEngineClientAutoConfiguration {

    private final ScriptEngineClientConfiguration clientConfig;
    private final RabbitTemplate rabbitTemplate;

    public ScriptEngineClientAutoConfiguration(AmqpAdmin amqpAdmin, RabbitTemplate rabbitTemplate, ScriptEngineClientConfiguration clientConfig) {
        this.clientConfig = clientConfig;
        this.rabbitTemplate = rabbitTemplate;
        amqpAdmin.declareQueue(new Queue(clientConfig.getResponseQueueName(), true, false, false));
        amqpAdmin.declareBinding(new Binding(clientConfig.getResponseQueueName(), Binding.DestinationType.QUEUE,
                "scriptengine_output", "scriptengine.output." + clientConfig.getClientName(), Map.of()));
    }

    @Bean
    public ScriptEngineClient scriptEngineClient() {
        return new ScriptEngineClient(clientConfig, rabbitTemplate);
    }

    @Bean
    public DirectMessageListenerContainer messageListenerContainer(ConnectionFactory connectionFactory) {
        DirectMessageListenerContainer container = new DirectMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.addQueueNames(clientConfig.getResponseQueueName());
        container.setMessageListener(scriptEngineClient());
        container.setPrefetchCount(100);  // TODO
        return container;
    }

}
