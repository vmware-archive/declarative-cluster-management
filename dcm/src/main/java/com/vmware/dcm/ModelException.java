/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

public class ModelException extends RuntimeException {
    public ModelException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public ModelException(final Exception e) {
        super(e);
    }

    public ModelException(final String message) {
        super(message);
    }
}