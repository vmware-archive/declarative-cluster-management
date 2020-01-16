/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm;

import io.fabric8.kubernetes.api.model.policy.PodDisruptionBudget;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import org.dcm.k8s.generated.Tables;
import org.jooq.DSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Listens to PodDisruptionBudget resources and updates the database accordingly
 */
public class PdbResourceEventHandler implements ResourceEventHandler<PodDisruptionBudget> {
    private static final Logger LOG = LoggerFactory.getLogger(PdbResourceEventHandler.class);
    private final DSLContext conn;

    PdbResourceEventHandler(final DSLContext conn) {
        this.conn = conn;
    }

    @Override
    public void onAdd(final PodDisruptionBudget pdb) {
        LOG.info("Adding PodDisruptionBudget {}", pdb);
        pdb.getSpec().getSelector().getMatchLabels().forEach(
            (k, v) ->
            conn.insertInto(Tables.PDB_MATCH_EXPRESSIONS)
                .values(pdb.getMetadata().getName(),
                        pdb.getSpec().getMinAvailable(),
                        pdb.getSpec().getMaxUnavailable(),
                        pdb.getStatus().getDisruptionsAllowed())
        );
        LOG.info("Added PodDisruptionBudget {}", pdb);
    }

    @Override
    public void onUpdate(final PodDisruptionBudget pdb, final PodDisruptionBudget t1) {

    }

    @Override
    public void onDelete(final PodDisruptionBudget pdb, final boolean b) {
        LOG.info("Deleting PodDisruptionBudget {}", pdb);
        pdb.getSpec().getSelector().getMatchLabels().forEach(
                (k, v) ->
                        conn.deleteFrom(Tables.PDB_MATCH_EXPRESSIONS)
                                .where(Tables.PDB_MATCH_EXPRESSIONS.PDB_NAME.eq(pdb.getMetadata().getName()))
        );
        LOG.info("Deleted PodDisruptionBudget {}", pdb);
    }
}
