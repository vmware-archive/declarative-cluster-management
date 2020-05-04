/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm;

public class ModelException extends RuntimeException {
    public ModelException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public ModelException(final String message) {
        super(message);
    }
}