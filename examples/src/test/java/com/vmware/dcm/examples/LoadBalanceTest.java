/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.examples;

import org.jooq.Record;
import org.jooq.Result;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LoadBalanceTest {
    private static final int NUM_PHYSICAL_MACHINES = 5;
    private static final int NUM_VIRTUAL_MACHINES = 10;

    /*
     * A simple constraint that forces all assignments to go the same node
     */
    @Test
    public void testSimpleConstraint() {
        final String allVmsGoToPm3 = "create view constraint_simple as " +
                                     "select * from virtual_machine " +
                                     "check controllable__physical_machine = 'pm3'";
        final LoadBalance lb = new LoadBalance(Collections.singletonList(allVmsGoToPm3));
        addInventory(lb);
        final Result<? extends Record> results = lb.run();
        System.out.println(results);
        results.forEach(e -> assertEquals("pm3", e.get("CONTROLLABLE__PHYSICAL_MACHINE")));
    }

    /*
     * We now add a capacity constraint to make sure that no physical machine is assigned more VMs
     * than it has capacity for. Given the constants we've chosen in addInventory(), there should be
     * at least two physical machines that receive VMs.
     */
    @Test
    public void testCapacityConstraints() {
        final String capacityConstraint =
                "create view constraint_capacity as " +
                "select * from virtual_machine " +
                "join physical_machine " +
                "  on physical_machine.name = virtual_machine.controllable__physical_machine " +
                "group by physical_machine.name, physical_machine.cpu_capacity, physical_machine.memory_capacity " +
                "check sum(virtual_machine.cpu) <= physical_machine.cpu_capacity and " +
                "       sum(virtual_machine.memory) <= physical_machine.memory_capacity";

        final LoadBalance lb = new LoadBalance(Collections.singletonList(capacityConstraint));
        addInventory(lb);
        final Result<? extends Record> results = lb.run();
        System.out.println(results);
        final Set<String> setOfPhysicalMachines = results.stream()
                                                     .map(e -> e.get("CONTROLLABLE__PHYSICAL_MACHINE", String.class))
                                                     .collect(Collectors.toSet());
        assertTrue(setOfPhysicalMachines.size() >= 2);
    }

    /*
     * Add a load balancing objective function. This should spread out VMs across all physical machines.
     */
    @Test
    public void testDistributeLoad() {
        final String capacityConstraint =
                "create view constraint_capacity as " +
                "select * from virtual_machine " +
                "join physical_machine " +
                "  on physical_machine.name = virtual_machine.controllable__physical_machine " +
                "group by physical_machine.name, physical_machine.cpu_capacity, physical_machine.memory_capacity " +
                "check sum(virtual_machine.cpu) <= physical_machine.cpu_capacity and " +
                "      sum(virtual_machine.memory) <= physical_machine.memory_capacity";

        final String spareCpu = "create view spare_cpu as " +
                "select physical_machine.cpu_capacity - sum(virtual_machine.cpu) as cpu_spare " +
                "from virtual_machine " +
                "join physical_machine " +
                "  on physical_machine.name = virtual_machine.controllable__physical_machine " +
                "group by physical_machine.name, physical_machine.cpu_capacity";

        // Queries presented as objectives, will have their values maximized.
        final String distributeLoadCpu = "create view objective_load_cpu as " +
                                         "select min(cpu_spare) from spare_cpu " +
                                         "maximize";

        final LoadBalance lb =
                new LoadBalance(List.of(capacityConstraint, spareCpu, distributeLoadCpu));
        addInventory(lb);
        final Result<? extends Record> result = lb.run();
        final Set<String> setOfPhysicalMachines = result.stream()
                .map(e -> e.get("CONTROLLABLE__PHYSICAL_MACHINE", String.class))
                .collect(Collectors.toSet());
        System.out.println(result);
        assertEquals(NUM_PHYSICAL_MACHINES, setOfPhysicalMachines.size());
    }

    private void addInventory(final LoadBalance lb) {
        // Add physical machines with CPU and Memory capacity as 50 units.
        for (int i = 0; i < NUM_PHYSICAL_MACHINES; i++) {
            lb.addPhysicalMachine("pm" + i, 50, 50);
        }

        // Add some VMs with CPU and Memory demand as 10 units.
        for (int i = 0; i < NUM_VIRTUAL_MACHINES; i++) {
            lb.addVirtualMachine("vm" + i, 10, 10);
        }
    }
}