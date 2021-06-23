/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.examples;

import com.vmware.dcm.SolverException;
import org.jooq.Record;
import org.jooq.Result;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

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
                                         "select * from spare_cpu " +
                                         "maximize min(cpu_spare)";

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

    /*
     * An example where we also refer to views computed in the database
     */
    @Test
    public void testDatabaseViews() {
        final String someVmsGoToPm3 = "create view constraint_simple as " +
                "select * from virtual_machine " +
                "check name not in (select name from vm_subset) or controllable__physical_machine = 'pm3'";

        final LoadBalance lb = new LoadBalance(List.of(someVmsGoToPm3));
        addInventory(lb);
        final Result<? extends Record> result = lb.run();
        result.stream().filter(e -> e.get("NAME").equals("vm1") || e.get("NAME").equals("vm2"))
              .forEach(e -> assertEquals("pm3", e.get("CONTROLLABLE__PHYSICAL_MACHINE")));
    }

    /*
     * We now introduce two mutually unsatisfiable constraints to showcase the UNSAT core API
     */
    @Test
    public void testUnsat() {
        // Satisfiable
        final String someVmsAvoidPm3 = "create view constraint_some_avoid_pm3 as " +
                "select * from virtual_machine " +
                "check name not in (select name from vm_subset) or controllable__physical_machine != 'pm3'";

        // The next two constraints are mutually unsatisfiable. The first constraint forces too many VMs
        // to go the same physical machine, but that violates the capacity constraint
        final String restGoToPm3 = "create view constraint_rest_to_pm3 as " +
                "select * from virtual_machine " +
                "check name in (select name from vm_subset) or controllable__physical_machine = 'pm3'";

        final String capacityConstraint =
                "create view constraint_capacity as " +
                "select * from virtual_machine " +
                "join physical_machine " +
                "  on physical_machine.name = virtual_machine.controllable__physical_machine " +
                "group by physical_machine.name, physical_machine.cpu_capacity, physical_machine.memory_capacity " +
                "check sum(virtual_machine.cpu) <= physical_machine.cpu_capacity and " +
                "       sum(virtual_machine.memory) <= physical_machine.memory_capacity";

        final LoadBalance lb = new LoadBalance(List.of(someVmsAvoidPm3, restGoToPm3, capacityConstraint));
        addInventory(lb);
        try {
            lb.run();
            fail();
        } catch (final SolverException e) {
            System.out.println(e.core());
            assertTrue(e.core().containsAll(List.of("constraint_rest_to_pm3", "constraint_capacity")));
            assertFalse(e.core().contains("constraint_some_avoid_pm3"));
        }
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