/**
 * Phaedra II
 *
 * Copyright (C) 2016-2022 Open Analytics
 *
 * ===========================================================================
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the Apache License as published by
 * The Apache Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Apache License for more details.
 *
 * You should have received a copy of the Apache License
 * along with this program.  If not, see <http://www.apache.org/licenses/>
 */
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
