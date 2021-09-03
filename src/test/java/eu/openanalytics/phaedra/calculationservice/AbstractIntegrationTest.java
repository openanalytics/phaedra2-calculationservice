package eu.openanalytics.phaedra.calculationservice;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.RequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;


@Testcontainers
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {CalculationService.class, IntegrationTestConfiguration.class})
@WebAppConfiguration
@AutoConfigureMockMvc
@TestPropertySource(locations = "classpath:application-test.properties")
abstract public class AbstractIntegrationTest {

    protected final ObjectMapper om;

    @Autowired
    protected MockMvc mockMvc;

    @DynamicPropertySource
    static void registerPgProperties(DynamicPropertyRegistry registry) {
        registry.add("DB_URL", Containers.postgreSQLContainer::getJdbcUrl);
        registry.add("DB_USER", Containers.postgreSQLContainer::getUsername);
        registry.add("DB_PASSWORD", Containers.postgreSQLContainer::getPassword);
    }

    public AbstractIntegrationTest() {
        om = new ObjectMapper();
        om.registerModule(new JavaTimeModule());
        om.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        om.setSerializationInclusion(JsonInclude.Include.NON_NULL); // ensure we don't send null values to the API (e.g. when doing updates)
        om.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
        om.configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
    }

    @Autowired
    private DataSource dataSource;

    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * Clean tables and sequences before every test (this is aster than restarting the container and Spring context).
     */
    @BeforeEach
    public void initEach() throws SQLException {
        try (Connection con = dataSource.getConnection()) {
            try (PreparedStatement stmt = con.prepareStatement("TRUNCATE formula RESTART IDENTITY CASCADE ;")) {
                stmt.executeUpdate();
            }
        }
    }

    protected <T> T performRequest(RequestBuilder requestBuilder, HttpStatus responseStatusCode, Class<T> resultType) throws Exception {
        var mvcResult = mockMvc.perform(requestBuilder).andReturn();

        Assertions.assertEquals("application/json", mvcResult.getResponse().getContentType());
        Assertions.assertEquals(responseStatusCode.value(), mvcResult.getResponse().getStatus(), "Status code is not expected value, returned body is " + mvcResult.getResponse().getContentAsString());

        Assertions.assertNotNull(mvcResult.getResponse().getContentAsString());
        var res = om.readValue(mvcResult.getResponse().getContentAsString(), resultType);
        Assertions.assertNotNull(res);
        return res;
    }

    protected <T> T performRequest(RequestBuilder requestBuilder, HttpStatus responseStatusCode, TypeReference<T> resultType) throws Exception {
        var mvcResult = mockMvc.perform(requestBuilder).andReturn();

        Assertions.assertEquals("application/json", mvcResult.getResponse().getContentType());
        Assertions.assertEquals(responseStatusCode.value(), mvcResult.getResponse().getStatus(), "Status code is not expected value, returned body is " + mvcResult.getResponse().getContentAsString());

        Assertions.assertNotNull(mvcResult.getResponse().getContentAsString());
        var res = om.readValue(mvcResult.getResponse().getContentAsString(), resultType);
        Assertions.assertNotNull(res);
        return res;
    }

    protected String performRequest(RequestBuilder requestBuilder, HttpStatus responseStatusCode) throws Exception {
        var mvcResult = mockMvc.perform(requestBuilder).andReturn();

        Assertions.assertEquals(responseStatusCode.value(), mvcResult.getResponse().getStatus(), "Status code is not expected value, returned body is " + mvcResult.getResponse().getContentAsString());
        if (!mvcResult.getResponse().getContentAsString().equals("")) {
            Assertions.assertEquals("application/json", mvcResult.getResponse().getContentType());
            // de-serialize and serialize responses in order to have a consistent response
            Object parsedConfig = om.readValue(mvcResult.getResponse().getContentAsString(), Object.class);
            return om.writeValueAsString(parsedConfig);
        }
        return null;
    }

    protected RequestBuilder post(String url, Object input) throws JsonProcessingException {
        return MockMvcRequestBuilders.post(url)
            .contentType("application/json")
            .content(om.writeValueAsString(input));
    }

    protected RequestBuilder put(String url, Object input) throws JsonProcessingException {
        return MockMvcRequestBuilders.put(url)
            .contentType("application/json")
            .content(om.writeValueAsString(input));
    }

}
