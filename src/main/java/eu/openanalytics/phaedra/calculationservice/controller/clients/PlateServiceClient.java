package eu.openanalytics.phaedra.calculationservice.controller.clients;

import eu.openanalytics.phaedra.calculationservice.model.Plate;

public interface PlateServiceClient {

    Plate getPlate(long plateId) throws PlateUnresolvableException;

}