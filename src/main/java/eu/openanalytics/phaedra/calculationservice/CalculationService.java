package eu.openanalytics.phaedra.calculationservice;

import eu.openanalytics.phaedra.util.jdbc.JDBCUtils;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.servers.Server;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.servlet.ServletContext;
import javax.sql.DataSource;

@SpringBootApplication
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
    public DataSource plateDataSource() {
        String url = environment.getProperty("DB_URL");
        String username = environment.getProperty("DB_USER");
        String password = environment.getProperty("DB_PASSWORD");

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
        return dataSource;
    }

    @Bean
    public OpenAPI customOpenAPI() {
        Server server = new Server().url(servletContext.getContextPath()).description("Default Server URL");
        return new OpenAPI().addServersItem(server);
    }
}
