package eu.openanalytics.phaedra.calculationservice.service.protocol;

import eu.openanalytics.phaedra.calculationservice.model.CalculationContext;
import org.slf4j.Logger;

public class ProtocolLogger {

    public static void log(Logger logger, CalculationContext cctx, String message, Object... formatArgs) {
        var prefix = String.format("Calculation [R=%s Pr=%s Pl=%s M=%s] ",  cctx.getResultSetId(), cctx.getProtocol().getId(), cctx.getPlate().getId(), cctx.getMeasId());
        logger.info(String.format(prefix + message, formatArgs));
    }

    public static void log(Logger logger, CalculationContext cctx, String message, Throwable ex, Object... formatArgs) {
        var prefix = String.format("Calculation [R=%s Pr=%s Pl=%s M=%s] ",  cctx.getResultSetId(), cctx.getProtocol().getId(), cctx.getPlate().getId(), cctx.getMeasId());
        logger.info(String.format(prefix + message, formatArgs), ex);
    }

}
