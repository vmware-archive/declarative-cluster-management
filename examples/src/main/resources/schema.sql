--
-- SQL schema for load balancing example
--

create table physical_machine (
    name varchar(30) primary key,
    cpu_capacity integer,
    memory_capacity integer
);

-- controllable__physical_machine represents a variable that the solver will assign values to
create table virtual_machine (
    name varchar(30) primary key not null,
    cpu  integer  not null,
    memory integer  not null,
    controllable__physical_machine varchar(30),
    foreign key (controllable__physical_machine) references physical_machine(name)
);

-- Constraints can refer to views computed in the database as well
create view vm_subset as
select * from virtual_machine where name ='vm1' or name = 'vm2';