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

    /**
     * Logs a message related to a protocol in a consistent format.
     *
     * Important: never pass user input to the formatString, this would be a security issue.
     * If you need to log an external string (without format parameters), pass "%s" as formatString and the string as
     * the sole formatArg.
     *
     * @param logger logger to use log the message to
     * @param cctx CalculationContext, used to generate a consistent prefix for the log messages
     * @param formatString a formatString for the logs message. You should NOT pass user input to this value.
     * @param formatArgs arguments for the formatString
     */
    public static void log(Logger logger, CalculationContext cctx, String formatString, Object... formatArgs) {
        if (formatArgs.length == 0) {
            throw new IllegalArgumentException("At least one formatArg is needed");
        }
        var prefix = String.format("Calculation [R=%s Pr=%s Pl=%s M=%s] ",  cctx.getResultSetId(), cctx.getProtocol().getId(), cctx.getPlate().getId(), cctx.getMeasId());
        logger.info(prefix + String.format(formatString, formatArgs));
    }

    /**
     *
     * Logs a message related to a protocol in a consistent format.
     *
     * @param logger logger to use log the message to
     * @param cctx CalculationContext, used to generate a consistent prefix for the log messages
     * @param message the message to log, may contain user input
     */
    public static void logMsg(Logger logger, CalculationContext cctx, String message) {
        var prefix = String.format("Calculation [R=%s Pr=%s Pl=%s M=%s] ",  cctx.getResultSetId(), cctx.getProtocol().getId(), cctx.getPlate().getId(), cctx.getMeasId());
        logger.info(prefix + message);
    }

    /**
     * Logs a message related to a protocol in a consistent format.
     *
     * Important: never pass user input to the formatString, this would be a security issue.
     * If you need to log an external string (without format parameters), pass "%s" as formatString and the string as
     * the sole formatArg.
     *
     * @param logger logger to use log the message to
     * @param cctx CalculationContext, used to generate a consistent prefix for the log messages
     * @param formatString a formatString for the logs message. You should NOT pass user input to this value.
     * @param ex the exception to log
     * @param formatArgs arguments for the formatString
     */
    public static void log(Logger logger, CalculationContext cctx, String formatString, Throwable ex, Object... formatArgs) {
        if (formatArgs.length == 0) {
            throw new IllegalArgumentException("At least one formatArg is needed");
        }
        var prefix = String.format("Calculation [R=%s Pr=%s Pl=%s M=%s] ",  cctx.getResultSetId(), cctx.getProtocol().getId(), cctx.getPlate().getId(), cctx.getMeasId());
        logger.info(prefix + String.format(formatString, formatArgs), ex);
    }

    /**
     *
     * Logs a message related to a protocol in a consistent format.
     *
     * @param logger logger to use log the message to
     * @param cctx CalculationContext, used to generate a consistent prefix for the log messages
     * @param message the message to log, may contain user input
     * @param ex the exception to log
     */
    public static void logMsg(Logger logger, CalculationContext cctx, String message, Throwable ex) {
        var prefix = String.format("Calculation [R=%s Pr=%s Pl=%s M=%s] ",  cctx.getResultSetId(), cctx.getProtocol().getId(), cctx.getPlate().getId(), cctx.getMeasId());
        logger.info(prefix + message, ex);
    }

}
