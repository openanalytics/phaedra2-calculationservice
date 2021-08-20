package eu.openanalytics.phaedra.calculationservice.controller.clients.impl;

import eu.openanalytics.phaedra.calculationservice.controller.clients.MeasServiceClient;
import eu.openanalytics.phaedra.calculationservice.controller.clients.MeasUnresolvableException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

@Component
public class HttpMeasServiceClient implements MeasServiceClient {

    private final RestTemplate restTemplate;

    public HttpMeasServiceClient(@Qualifier("unLoadBalancedRestTemplate") RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    @Override
    public float[] getWellData(long measId, String columnName) throws MeasUnresolvableException {
        try {
            var wellData = restTemplate.getForObject(UrlFactory.measurementWell(measId, columnName), float[].class);
            if (wellData == null) {
                throw new MeasUnresolvableException("WellData could not be converted");
            }
            return wellData;
        } catch (HttpClientErrorException.NotFound ex) {
            throw new MeasUnresolvableException("WellData not found");
        } catch (HttpClientErrorException ex) {
            throw new MeasUnresolvableException("Error while fetching WellData");
        }
    }

}
