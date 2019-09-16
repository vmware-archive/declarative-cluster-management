/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

/**
 * This informs FindBugs to mark parameters, fields and methods as NonNull.
 */

@ParametersAreNonnullByDefault
@FieldsAreNonnullByDefault
@MethodsAreNonnullByDefault

package org.dcm.backend;

import org.dcm.annotations.FieldsAreNonnullByDefault;
import org.dcm.annotations.MethodsAreNonnullByDefault;

import javax.annotation.ParametersAreNonnullByDefault;

