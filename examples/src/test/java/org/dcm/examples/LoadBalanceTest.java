/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.examples;

import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class LoadBalanceTest {

    static {
        System.getProperties().setProperty("org.jooq.no-logo", "true");
    }

    private final String capacityConstraint =
            "create view constraint_capacity as\n" +
            "select * from virtual_machine\n" +
            "join physical_machine\n" +
            "  on physical_machine.name = virtual_machine.controllable__physical_machine\n" +
            "group by physical_machine.name, physical_machine.cpu_capacity, physical_machine.memory_capacity\n" +
            "having sum(virtual_machine.cpu) <= physical_machine.cpu_capacity and\n" +
            "       sum(virtual_machine.memory) <= physical_machine.memory_capacity";


    /*
     * We don't supply any constraints. So the solver will arbitrarily pick a few nodes to assign
     * these VMs to.
     */
    @Test
    public void testNoConstraints() {
        final LoadBalance lb = new LoadBalance(Collections.emptyList());
        addInventory(lb);
        lb.run();
    }


    /*
     * We don't supply any constraints. So the solver will arbitrarily pick a few nodes to assign
     * these VMs to.
     */
    @Test
    public void testSimpleConstraint() {
        final LoadBalance lb = new LoadBalance(Collections.emptyList());
        addInventory(lb);
        lb.run();
    }


    /*
     * We now add a capacity constraint to make sure that no physical machine is assigned more VMs
     * than it has capacity for.
     */
    @Test
    public void testCapacityConstraints() {
        final LoadBalance lb = new LoadBalance(Collections.singletonList(capacityConstraint));
        addInventory(lb);
        lb.run();
    }


    /*
     * Add a load balancing objective function.
     */
    @Test
    public void testDistributeLoad() {
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
        lb.run();
    }

    private void addInventory(final LoadBalance lb) {
        // Add physical machines with CPU and Memory capacity as 100 units.
        int numPhysicalMachines = 5;
        for (int i = 0; i < numPhysicalMachines; i++) {
            lb.addNode("pm" + i, 50, 50);
        }

        // Add some VMs with CPU and Memory demand as 10 units.
        int numVirtualMachines = 10;
        for (int i = 0; i < numVirtualMachines; i++) {
            lb.addVm("vm" + i, 10, 10);
        }
    }
}