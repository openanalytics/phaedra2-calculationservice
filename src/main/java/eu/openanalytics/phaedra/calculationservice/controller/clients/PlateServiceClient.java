package eu.openanalytics.phaedra.calculationservice.controller.clients;

import eu.openanalytics.phaedra.model.v2.runtime.Plate;

public interface PlateServiceClient {

    Plate getPlate(long plateId) throws PlateUnresolvableException;

}