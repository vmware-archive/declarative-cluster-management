/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

/**
 * This informs FindBugs to mark parameters, fields and methods as NonNull.
 */

@ParametersAreNonnullByDefault
@FieldsAreNonnullByDefault
@MethodsAreNonnullByDefault

package com.vmware.dcm.compiler;

import com.vmware.dcm.annotations.FieldsAreNonnullByDefault;
import com.vmware.dcm.annotations.MethodsAreNonnullByDefault;

import javax.annotation.ParametersAreNonnullByDefault;

