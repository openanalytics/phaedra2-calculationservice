package eu.openanalytics.phaedra.calculationservice;

import eu.openanalytics.phaedra.measurementservice.client.config.MeasurementServiceClientAutoConfiguration;
import eu.openanalytics.phaedra.platservice.client.config.PlateServiceClientAutoConfiguration;
import eu.openanalytics.phaedra.protocolservice.client.config.ProtocolServiceClientAutoConfiguration;
import eu.openanalytics.phaedra.resultdataservice.client.config.ResultDataServiceClientAutoConfiguration;
import eu.openanalytics.phaedra.scriptengine.client.config.ScriptEngineClientAutoConfiguration;
import eu.openanalytics.phaedra.scriptengine.client.config.ScriptEngineClientConfiguration;
import eu.openanalytics.phaedra.scriptengine.client.model.TargetRuntime;
import eu.openanalytics.phaedra.util.PhaedraRestTemplate;
import eu.openanalytics.phaedra.util.jdbc.JDBCUtils;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.servers.Server;
import liquibase.integration.spring.SpringLiquibase;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.client.loadbalancer.LoadBalanced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.scheduling.annotation.EnableScheduling;

import javax.servlet.ServletContext;
import javax.sql.DataSource;
import java.time.Clock;

@SpringBootApplication
@EnableDiscoveryClient
@EnableScheduling
@Import({ScriptEngineClientAutoConfiguration.class,
        ProtocolServiceClientAutoConfiguration.class,
        ResultDataServiceClientAutoConfiguration.class,
        PlateServiceClientAutoConfiguration.class,
        MeasurementServiceClientAutoConfiguration.class})
public class CalculationService {
    private final ServletContext servletContext;
    private final Environment environment;

    public CalculationService(ServletContext servletContext, Environment environment) {
        this.servletContext = servletContext;
        this.environment = environment;
    }

    public static void main(String[] args) {
        SpringApplication.run(CalculationService.class, args);
    }

    @Bean
    public DataSource dataSource() {
        String url = environment.getProperty("DB_URL");
        String username = environment.getProperty("DB_USER");
        String password = environment.getProperty("DB_PASSWORD");
        String schema = environment.getProperty("DB_SCHEMA");

        if (StringUtils.isEmpty(url)) {
            throw new RuntimeException("No database URL configured: " + url);
        }
        String driverClassName = JDBCUtils.getDriverClassName(url);
        if (driverClassName == null) {
            throw new RuntimeException("Unsupported database type: " + url);
        }

        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName(driverClassName);
        dataSource.setUrl(url);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        if (!StringUtils.isEmpty(schema)) {
            dataSource.setSchema(schema);
        }
        return dataSource;
    }

    @Bean
    @LoadBalanced
    public PhaedraRestTemplate restTemplate() {
        return new PhaedraRestTemplate();
    }

    @Bean
    public SpringLiquibase liquibase() {
        SpringLiquibase liquibase = new SpringLiquibase();
        liquibase.setChangeLog("classpath:liquibase-changeLog.xml");

        String schema = environment.getProperty("DB_SCHEMA");
        if (!StringUtils.isEmpty(schema)) {
            liquibase.setDefaultSchema(schema);
        }

        liquibase.setDataSource(dataSource());
        return liquibase;
    }

    @Bean
    public OpenAPI customOpenAPI() {
        Server server = new Server().url(servletContext.getContextPath()).description("Default Server URL");
        return new OpenAPI().addServersItem(server);
    }

    public static final String R_FAST_LANE = "R_FAST_LANE";
    public static final String JAVASTAT_FAST_LANE = "JAVASTAT_FAST_LANE";

    @Bean
    public ScriptEngineClientConfiguration scriptEngineConfiguration() {
        var config = new ScriptEngineClientConfiguration();
        config.setClientName("CalculationService");
        config.addTargetRuntime(R_FAST_LANE, new TargetRuntime("R", "fast-lane", "v1"));
        config.addTargetRuntime(JAVASTAT_FAST_LANE, new TargetRuntime("JavaStat", "fast-lane", "v1"));
        return config;
    }

    @Bean
    public Clock clock() {
        return Clock.systemDefaultZone();
    }

}
