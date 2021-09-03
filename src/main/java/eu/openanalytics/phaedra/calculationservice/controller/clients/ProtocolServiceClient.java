package eu.openanalytics.phaedra.calculationservice.controller.clients;

import eu.openanalytics.phaedra.model.v2.runtime.Protocol;

public interface ProtocolServiceClient {

    Protocol getProtocol(long protocolId) throws ProtocolUnresolvableException;

}