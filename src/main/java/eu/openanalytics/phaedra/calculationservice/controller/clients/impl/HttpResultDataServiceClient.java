package eu.openanalytics.phaedra.calculationservice.controller.clients.impl;

import eu.openanalytics.phaedra.calculationservice.controller.clients.ResultDataServiceClient;
import eu.openanalytics.phaedra.calculationservice.controller.clients.ResultDataUnresolvableException;
import eu.openanalytics.phaedra.calculationservice.controller.clients.ResultSetUnresolvableException;
import eu.openanalytics.phaedra.calculationservice.dto.external.ErrorDTO;
import eu.openanalytics.phaedra.calculationservice.dto.external.PageDTO;
import eu.openanalytics.phaedra.calculationservice.dto.external.ResultDataDTO;
import eu.openanalytics.phaedra.calculationservice.dto.external.ResultSetDTO;
import eu.openanalytics.phaedra.calculationservice.scriptengineclient.model.ResponseStatusCode;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;

import java.util.List;
import java.util.Objects;

@Component
public class HttpResultDataServiceClient implements ResultDataServiceClient {

    private final ERestTemplate restTemplate;

    private final static ParameterizedTypeReference<PageDTO<ResultDataDTO>> PAGED_RESULTDATA_TYPE = new ParameterizedTypeReference<>() {
    };

    public HttpResultDataServiceClient(ERestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    @Override
    public ResultSetDTO createResultDataSet(long protocolId, long plateId, long measId) throws ResultSetUnresolvableException {
        var resultSet = ResultSetDTO.builder()
                .protocolId(protocolId)
                .plateId(plateId)
                .measId(measId)
                .build();

        try {
            var res = restTemplate.postForObject(UrlFactory.resultSet(), resultSet, ResultSetDTO.class);
            if (res == null) {
                throw new ResultSetUnresolvableException("ResultSet could not be converted");
            }
            return res;
        } catch (HttpClientErrorException ex) {
            throw new ResultSetUnresolvableException("Error while creating ResultSet", ex);
        } catch (HttpServerErrorException ex) {
            throw new ResultSetUnresolvableException("Server Error while creating ResultSet", ex);
        }
    }

    @Override
    public ResultSetDTO completeResultDataSet(long resultSetId, String outcome, List<ErrorDTO> errors, String errorsText) throws ResultSetUnresolvableException {
        Objects.requireNonNull(outcome, "Outcome may not be null");
        var resultSet = ResultSetDTO.builder()
                .outcome(outcome)
                .errors(errors)
                .errorsText(errorsText)
                .build();

        try {
            var res = restTemplate.putForObject(UrlFactory.resultSet(resultSetId), resultSet, ResultSetDTO.class);
            if (res == null) {
                throw new ResultSetUnresolvableException("ResultSet could not be converted");
            }
            return res;
        } catch (HttpClientErrorException ex) {
            throw new ResultSetUnresolvableException("Error while creating ResultSet", ex);
        }
    }

    @Override
    public ResultDataDTO addResultData(long resultSetId, long featureId, float[] values, ResponseStatusCode statusCode, String statusMessage, Integer exitCode) throws ResultDataUnresolvableException {
        Objects.requireNonNull(values, "Values may not be null");
        Objects.requireNonNull(statusCode, "StatusCode may not be null");
        Objects.requireNonNull(statusMessage, "StatusMessage may not be null");
        Objects.requireNonNull(exitCode, "ExitCode may not be null");

        var resultData = ResultDataDTO.builder()
                .featureId(featureId)
                .values(values)
                .statusCode(statusCode)
                .statusMessage(statusMessage)
                .exitCode(exitCode)
                .build();

        try {
            var res = restTemplate.postForObject(UrlFactory.resultData(resultSetId), resultData, ResultDataDTO.class);
            if (res == null) {
                throw new ResultDataUnresolvableException("ResultData could not be converted");
            }
            return res;
        } catch (HttpClientErrorException ex) {
            throw new ResultDataUnresolvableException("Error while creating ResultData", ex);
        }
    }

    @Override
    public ResultDataDTO getResultData(long resultSetId, long featureId) throws ResultDataUnresolvableException {
        try {
            var resultData = restTemplate.getForObject(UrlFactory.resultDataByFeatureId(resultSetId, featureId), PAGED_RESULTDATA_TYPE);

            if (resultData == null) {
                throw new ResultDataUnresolvableException("ResultData could not be converted");
            }
            if (resultData.getStatus().getTotalElements() == 0) {
                throw new ResultDataUnresolvableException("ResultData did not contain any data");
            }
            if (resultData.getStatus().getTotalElements() > 1) {
                throw new ResultDataUnresolvableException("ResultData did contain too many data");
            }
            return resultData.getData().get(0);
        } catch (HttpClientErrorException.NotFound ex) {
            throw new ResultDataUnresolvableException("ResultData not found");
        } catch (HttpClientErrorException ex) {
            throw new ResultDataUnresolvableException("Error while fetching ResultData");
        }
    }


}
