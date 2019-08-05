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

package com.vrg;

import com.vrg.annotations.FieldsAreNonnullByDefault;
import com.vrg.annotations.MethodsAreNonnullByDefault;

import javax.annotation.ParametersAreNonnullByDefault;

