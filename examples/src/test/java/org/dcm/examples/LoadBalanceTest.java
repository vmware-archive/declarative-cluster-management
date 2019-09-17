/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.examples;

import org.jooq.Record;
import org.jooq.Result;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LoadBalanceTest {
    private static final int NUM_PHYSICAL_MACHINES = 5;
    private static final int NUM_VIRTUAL_MACHINES = 10;

    static {
        System.getProperties().setProperty("org.jooq.no-logo", "true");
    }

    /*
     * We don't supply any constraints. So the solver will arbitrarily pick a few nodes to assign
     * these VMs to.
     */
    @Test
    public void testNoConstraints() {
        final LoadBalance lb = new LoadBalance(Collections.emptyList());
        addInventory(lb);
        final Result<? extends Record> results = lb.run();
        assertEquals(NUM_VIRTUAL_MACHINES, results.size());
    }

    /*
     * A simple constraint that forces all assignments to go the same node
     */
    @Test
    public void testSimpleConstraint() {
        final String allVmsGoToPm3 = "create view constraint_simple as\n" +
                                     "select * from virtual_machine\n" +
                                     "where controllable__physical_machine = 'pm3'";
        final LoadBalance lb = new LoadBalance(Collections.singletonList(allVmsGoToPm3));
        addInventory(lb);
        final Result<? extends Record> results = lb.run();
        results.forEach(e -> assertEquals(e.get("CONTROLLABLE__PHYSICAL_MACHINE"), "pm3"));
    }

    /*
     * We now add a capacity constraint to make sure that no physical machine is assigned more VMs
     * than it has capacity for. Given the constants we've chosen in addInventory(), there should be
     * at least two physical machines that receive VMs.
     */
    @Test
    public void testCapacityConstraints() {
        final String capacityConstraint =
                "create view constraint_capacity as\n" +
                "select * from virtual_machine\n" +
                "join physical_machine\n" +
                "  on physical_machine.name = virtual_machine.controllable__physical_machine\n" +
                "group by physical_machine.name, physical_machine.cpu_capacity, physical_machine.memory_capacity\n" +
                "having sum(virtual_machine.cpu) <= physical_machine.cpu_capacity and\n" +
                "       sum(virtual_machine.memory) <= physical_machine.memory_capacity";

        final LoadBalance lb = new LoadBalance(Collections.singletonList(capacityConstraint));
        addInventory(lb);
        final Result<? extends Record> results = lb.run();
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
                "create view constraint_capacity as\n" +
                "select * from virtual_machine\n" +
                "join physical_machine\n" +
                "  on physical_machine.name = virtual_machine.controllable__physical_machine\n" +
                "group by physical_machine.name, physical_machine.cpu_capacity, physical_machine.memory_capacity\n" +
                "having sum(virtual_machine.cpu) <= physical_machine.cpu_capacity and\n" +
                "       sum(virtual_machine.memory) <= physical_machine.memory_capacity";

        final String load = "create view load as\n" +
                "select sum(virtual_machine.cpu) as cpu_load, sum(virtual_machine.memory) as mem_load\n" +
                "from virtual_machine\n" +
                "join physical_machine\n" +
                "  on physical_machine.name = virtual_machine.controllable__physical_machine\n" +
                "group by physical_machine.name, physical_machine.cpu_capacity, physical_machine.memory_capacity";

        // Queries presented as objectives, will have their values maximized. If there are multiple objectives,
        // the sum of their values will be maximized.
        final String distributeLoadCpu = "create view objective_load_cpu as select min(cpu_load) from load";
        final String distributeLoadMemory = "create view objective_load_mem as select min(mem_load) from load";

        final LoadBalance lb =
                  new LoadBalance(List.of(capacityConstraint, load, distributeLoadCpu, distributeLoadMemory));
        addInventory(lb);
        final Result<? extends Record> result = lb.run();
        final Set<String> setOfPhysicalMachines = result.stream()
                .map(e -> e.get("CONTROLLABLE__PHYSICAL_MACHINE", String.class))
                .collect(Collectors.toSet());
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