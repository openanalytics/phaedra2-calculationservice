package eu.openanalytics.phaedra.calculationservice.controller.clients.impl;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.lang.Nullable;
import org.springframework.web.client.HttpMessageConverterExtractor;
import org.springframework.web.client.RequestCallback;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.lang.reflect.ParameterizedType;

// TODO move this class to somewhere else
public class ERestTemplate extends RestTemplate {

    // copy of postForObject but for put
    @Nullable
    public <T> T putForObject(String url, @Nullable Object request, Class<T> responseType,
                              Object... uriVariables) throws RestClientException {

        RequestCallback requestCallback = httpEntityCallback(request, responseType);
        HttpMessageConverterExtractor<T> responseExtractor = new HttpMessageConverterExtractor<>(responseType, getMessageConverters());
        return execute(url, HttpMethod.PUT, requestCallback, responseExtractor, uriVariables);
    }

    /**
     * Copy of getForObject but for a ParameterizedTypeReference, allowing to fetch a Generic type.
     * E.g. {@code List<Object> }
     */
    @Nullable
    public <T> T getForObject(String url, ParameterizedTypeReference<T> type, Object... uriVariables) {
        var clazz = ((ParameterizedType) type.getType()).getRawType().getClass();
        RequestCallback requestCallback = acceptHeaderRequestCallback(clazz);
        HttpMessageConverterExtractor<T> responseExtractor = new HttpMessageConverterExtractor<>(type.getType(), getMessageConverters());
        return execute(url, HttpMethod.GET, requestCallback, responseExtractor, uriVariables);
    }
}
