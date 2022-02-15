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
package eu.openanalytics.phaedra.calculationservice.model;

import java.util.Optional;

/**
 * TODO
 */
public class SuccessTracker<T> {

    private boolean success = true;

    private Optional<T> result = Optional.empty();

    public void failedIfStepFailed(SuccessTracker<?> success) {
        if (!success.isSuccess()) {
            this.success = false;
        }
    }

    public boolean isSuccess() {
        return success;
    }

    public void failed() {
        success = false;
    }

    public void setResult(T result) {
        this.result = Optional.of(result);
    }

    public T getResult() {
        return result.get();
    }

}


