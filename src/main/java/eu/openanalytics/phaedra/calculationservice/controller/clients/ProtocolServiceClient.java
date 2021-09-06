package eu.openanalytics.phaedra.calculationservice.controller.clients;

import eu.openanalytics.phaedra.calculationservice.model.Protocol;

public interface ProtocolServiceClient {

    Protocol getProtocol(long protocolId) throws ProtocolUnresolvableException;

}