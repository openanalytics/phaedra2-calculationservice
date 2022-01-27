package eu.openanalytics.phaedra.calculationservice.config;

import eu.openanalytics.phaedra.calculationservice.service.TokenService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;

import java.io.IOException;

public class RestTemplateHeaderModifier implements ClientHttpRequestInterceptor {

    @Autowired
    private TokenService tokenService;

    @Override
    public ClientHttpResponse intercept(HttpRequest httpRequest,
                                        byte[] bytes,
                                        ClientHttpRequestExecution execution) throws IOException {
        if (httpRequest.getHeaders().containsKey(HttpHeaders.AUTHORIZATION)) {
            return execution.execute(httpRequest, bytes);
        } else {
            var bearerToken = tokenService.getAuthorisationToken();
            httpRequest.getHeaders().add(HttpHeaders.AUTHORIZATION, bearerToken);
            return execution.execute(httpRequest, bytes);
        }
    }
}
