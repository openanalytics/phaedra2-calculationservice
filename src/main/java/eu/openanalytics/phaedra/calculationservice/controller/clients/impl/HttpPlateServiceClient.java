package eu.openanalytics.phaedra.calculationservice.controller.clients.impl;

import eu.openanalytics.phaedra.calculationservice.controller.clients.PlateServiceClient;
import eu.openanalytics.phaedra.calculationservice.controller.clients.PlateUnresolvableException;
import eu.openanalytics.phaedra.calculationservice.dto.external.PlateDTO;
import eu.openanalytics.phaedra.calculationservice.model.Plate;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

@Component
public class HttpPlateServiceClient implements PlateServiceClient {

    private static final String PHAEDRA_PLATE_SERVICE = "http://PHAEDRA-PLATE-SERVICE/phaedra/plate-service";

    private final RestTemplate restTemplate;

    public HttpPlateServiceClient(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    @Override
    public Plate getPlate(long plateId) throws PlateUnresolvableException {
        // 1. get plate
        try {
            var plate = restTemplate.getForObject(PHAEDRA_PLATE_SERVICE + "/plate/" + plateId, PlateDTO.class);
            if (plate == null) {
                throw new PlateUnresolvableException("Plate could not be converted");
            }

            return new Plate(
                    plate.getId(),
                    plate.getBarcode(),
                    plate.getDescription(),
                    plate.getExperimentId(),
                    plate.getRows(),
                    plate.getColumns(),
                    plate.getSequence(),
                    plate.getLinkStatus(),
                    plate.getLinkSource(),
                    plate.getLinkTemplateId(),
                    plate.getLinkedOn(),
                    plate.getCalculationStatus(),
                    plate.getCalculationError(),
                    plate.getCalculatedBy(),
                    plate.getCalculatedOn(),
                    plate.getValidationStatus(),
                    plate.getValidatedBy(),
                    plate.getValidatedOn(),
                    plate.getApprovalStatus(),
                    plate.getApprovedBy(),
                    plate.getApprovedOn(),
                    plate.getUploadStatus(),
                    plate.getUploadedBy(),
                    plate.getUploadedOn(),
                    plate.getCreatedOn(),
                    plate.getCreatedBy(),
                    plate.getUpdatedOn(),
                    plate.getUpdatedBy()
            );

        } catch (
                HttpClientErrorException.NotFound ex) {
            throw new PlateUnresolvableException("Plate not found");
        } catch (
                HttpClientErrorException ex) {
            throw new PlateUnresolvableException("Error while fetching plate");
        }
    }


}
