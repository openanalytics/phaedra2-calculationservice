/**
 * Phaedra II
 *
 * Copyright (C) 2016-2022 Open Analytics
 *
 * ===========================================================================
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the Apache License as published by
 * The Apache Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Apache License for more details.
 *
 * You should have received a copy of the Apache License
 * along with this program.  If not, see <http://www.apache.org/licenses/>
 */
package eu.openanalytics.phaedra.calculationservice;

import java.time.Clock;

import javax.sql.DataSource;

import eu.openanalytics.phaedra.curvedataservice.client.config.CurveDataServiceClientAutoConfiguration;
import eu.openanalytics.phaedra.metadataservice.client.config.MetadataServiceClientAutoConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.client.loadbalancer.LoadBalanced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.oauth2.client.registration.ClientRegistrationRepository;
import org.springframework.security.web.SecurityFilterChain;

import eu.openanalytics.phaedra.measurementservice.client.config.MeasurementServiceClientAutoConfiguration;
import eu.openanalytics.phaedra.plateservice.client.config.PlateServiceClientAutoConfiguration;
import eu.openanalytics.phaedra.protocolservice.client.config.ProtocolServiceClientAutoConfiguration;
import eu.openanalytics.phaedra.resultdataservice.client.config.ResultDataServiceClientAutoConfiguration;
import eu.openanalytics.phaedra.scriptengine.client.config.ScriptEngineClientAutoConfiguration;
import eu.openanalytics.phaedra.scriptengine.client.config.ScriptEngineClientConfiguration;
import eu.openanalytics.phaedra.scriptengine.client.model.TargetRuntime;
import eu.openanalytics.phaedra.util.PhaedraRestTemplate;
import eu.openanalytics.phaedra.util.auth.AuthenticationConfigHelper;
import eu.openanalytics.phaedra.util.auth.AuthorizationServiceFactory;
import eu.openanalytics.phaedra.util.auth.ClientCredentialsTokenGenerator;
import eu.openanalytics.phaedra.util.auth.IAuthorizationService;
import eu.openanalytics.phaedra.util.jdbc.JDBCUtils;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.servers.Server;
import liquibase.integration.spring.SpringLiquibase;

@SpringBootApplication
@EnableDiscoveryClient
@EnableScheduling
@EnableWebSecurity
@EnableKafka
@Import({ScriptEngineClientAutoConfiguration.class,
        ProtocolServiceClientAutoConfiguration.class,
        ResultDataServiceClientAutoConfiguration.class,
        CurveDataServiceClientAutoConfiguration.class,
        MetadataServiceClientAutoConfiguration.class,
        PlateServiceClientAutoConfiguration.class,
        MeasurementServiceClientAutoConfiguration.class})
public class CalculationService {

    private final Environment environment;

    public CalculationService(Environment environment) {
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
        PhaedraRestTemplate restTemplate = new PhaedraRestTemplate();
        return restTemplate;
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
        Server server = new Server().url(environment.getProperty("API_URL")).description("Default Server URL");
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

    @Bean
    public ClientCredentialsTokenGenerator ccTokenGenerator(ClientRegistrationRepository clientRegistrationRepository) {
    	return new ClientCredentialsTokenGenerator("keycloak", clientRegistrationRepository);
    }

	@Bean
	public IAuthorizationService authService(ClientCredentialsTokenGenerator ccTokenGenerator) {
		return AuthorizationServiceFactory.create(ccTokenGenerator);
	}

	@Bean
	public SecurityFilterChain httpSecurity(HttpSecurity http) throws Exception {
		return AuthenticationConfigHelper.configure(http);
	}
}
