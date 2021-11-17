/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import edu.umd.cs.findbugs.annotations.Nullable;
import io.fabric8.kubernetes.api.model.Quantity;

class Utils {

    static long convertUnit(final Quantity quantity, final String resourceName) {
        if (resourceName.equals("cpu")) {
            final String unit = quantity.getFormat();
            if (unit.equals("")) {
                // Convert to milli-cpu
                return (long) (Double.parseDouble(quantity.getAmount()) * 1000);
            } else if (unit.equals("m")) {
                return (long) (Double.parseDouble(quantity.getAmount()));
            }
            throw new IllegalArgumentException(quantity + " for resource type: " + resourceName);
        } else {
            // Values are guaranteed to be under 2^63 - 1, so this is safe
            return Quantity.getAmountInBytes(quantity).longValue();
        }
    }

    static void compileDDlog(@Nullable String ddlogFile) {
        final DDlogDBConnectionPool dbConnectionPool = new DDlogDBConnectionPool(ddlogFile);
        dbConnectionPool.buildDDlog(false, false);
        final Scheduler scheduler = new Scheduler.Builder(dbConnectionPool)
                .setScopedInitialPlacement(true).build();
        dbConnectionPool.buildDDlog(true, true);
    }
}