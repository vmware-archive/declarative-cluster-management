create table node_info
(
  name varchar(36) not null primary key,
  isMaster boolean not null,
  unschedulable boolean not null,
  out_of_disk boolean not null,
  memory_pressure boolean not null,
  disk_pressure boolean not null,
  pid_pressure boolean not null,
  ready boolean not null,
  network_unavailable boolean not null,
  cpu_capacity bigint not null,
  memory_capacity bigint not null,
  ephemeral_storage_capacity bigint not null,
  pods_capacity bigint not null,
  cpu_allocatable bigint not null,
  memory_allocatable bigint not null,
  ephemeral_storage_allocatable bigint not null,
  pods_allocatable bigint not null
);

create table pod_info
(
  pod_name varchar(100) not null primary key,
  status varchar(36) not null,
  node_name varchar(36) null,
  namespace varchar(100) not null,
  cpu_request bigint not null,
  memory_request bigint not null,
  ephemeral_storage_request bigint not null,
  pods_request bigint not null,
  owner_name varchar(100) not null,
  creation_timestamp varchar(100) not null,
  priority integer not null
);

-- This table tracks the "ContainerPorts" fields of each pod.
-- It is used to enforce the PodFitsHostPorts constraint.
create table pod_ports_request
(
  pod_name varchar(100) not null,
  host_ip varchar(100) not null,
  host_port integer not null,
  host_protocol varchar(10) not null,
  foreign key(pod_name) references pod_info(pod_name)
);

-- This table tracks the set of hostports in use at each node.
-- Also used to enforce the PodFitsHostPorts constraint.
create table container_host_ports
(
  pod_name varchar(100) not null,
  node_name varchar(36) not null,
  host_ip varchar(100) not null,
  host_port integer not null,
  host_protocol varchar(10) not null,
  foreign key(pod_name) references pod_info(pod_name),
  foreign key(node_name) references node_info(name)
);

-- Tracks the set of node selector labels per pod.
-- Also used for node affinity (IN)
create table pod_node_selector_labels
(
  pod_name varchar(100) not null,
  label_key varchar(100) not null,
  label_value varchar(36) not null,
  operator varchar(30) not null,
  foreign key(pod_name) references pod_info(pod_name)
);

-- Tracks the set of pod affinity match expressions.
create table pod_affinity_match_expressions
(
  pod_name varchar(100) not null,
  label_key varchar(100) not null,
  label_value varchar(36) not null,
  operator varchar(30) not null,
  topology_key varchar(100) not null,
  foreign key(pod_name) references pod_info(pod_name)
);

-- Tracks the set of pod anti-affinity match expressions.
create table pod_anti_affinity_match_expressions
(
  pod_name varchar(100) not null,
  label_key varchar(100) not null,
  label_value varchar(36) not null,
  operator varchar(30) not null,
  topology_key varchar(100) not null,
  foreign key(pod_name) references pod_info(pod_name)
);


-- Tracks the set of labels per pod, and indicates if
-- any of them are also node selector labels
create table pod_labels
(
  pod_name varchar(100) not null,
  label_key varchar(100) not null,
  label_value varchar(36) not null,
  is_selector boolean not null,
  foreign key(pod_name) references pod_info(pod_name)
);

-- Tracks the set of labels per node
create table node_labels
(
  node_name varchar(36) not null,
  label_key varchar(100) not null,
  label_value varchar(36) not null,
  foreign key(node_name) references node_info(name)
);

-- Volume labels
create table volume_labels
(
  volume_name varchar(36) not null,
  pod_name varchar(100) not null,
  label_key varchar(100) not null,
  label_value varchar(36) not null,
  foreign key(pod_name) references pod_info(pod_name)
);

-- For pods that have ports exposed
create table pod_by_service
(
  pod_name varchar(100) not null,
  service_name varchar(100) not null,
  foreign key(pod_name) references pod_info(pod_name)
);

-- Service affinity labels
create table service_affinity_labels
(
  label_key varchar(100) not null
);


-- Labels present on node
create table labels_to_check_for_presence
(
  label_key varchar(100) not null,
  present boolean not null
);

-- Node taints
create table node_taints
(
  node_name varchar(36) not null,
  taint_key varchar(100) not null,
  taint_value varchar(100),
  taint_effect varchar(100) not null,
  foreign key(node_name) references node_info(name)
);

-- Pod taints.
create table pod_tolerations
(
  pod_name varchar(100) not null,
  taint_key varchar(100) not null,
  taint_value varchar(100) null,
  taint_effect varchar(100) not null,
  taint_operator varchar(100) not null,
  foreign key(pod_name) references pod_info(pod_name)
);

-- Tracks the set of node images that are already
-- available at a node
create table node_images
(
  node_name varchar(36) not null,
  image_name varchar(200) not null,
  image_size bigint not null,
  foreign key(node_name) references node_info(name)
);

-- Tracks the container images required by each pod
create table pod_images
(
  pod_name varchar(100) not null,
  image_name varchar(200) not null,
  foreign key(pod_name) references pod_info(pod_name)
);

-- Tracks the number of pods per node per group
create table pods_per_node_per_group
(
  owner_name varchar(100) not null primary key,
  pods_limit integer not null
);

-- This view finds all services that have pending pods and have label keys
-- that appear in the service_affinity_labels table.
create view pending_services_with_affinity_labels as
select pod_by_service.service_name as service_name,
       pod_info.node_name as node_name
from pod_by_service
join pod_info
     on pod_info.pod_name = pod_by_service.pod_name
     and pod_info.status = 'Pending'
join pod_labels
     on pod_info.pod_name = pod_labels.pod_name
     and pod_labels.label_key in (select service_affinity_labels.label_key from service_affinity_labels);


-- Helper view used for the ImageLocality policy.
create view image_locality as
select pod_info.pod_name,
       node_images.node_name as node_name,
       sum(node_images.image_size) as image_size
from pod_info
join pod_images
    on pod_info.pod_name = pod_images.pod_name
join node_images
     on pod_images.image_name = node_images.image_name
group by pod_info.pod_name, node_images.node_name;

-- Helper view used for checking for valid nodes
create view valid_node_set as
select node_labels.node_name as node_name from node_labels
join labels_to_check_for_presence
    on node_labels.label_key = labels_to_check_for_presence.label_key
group by node_labels.node_name
having count(node_labels.label_key) = (select count(*) from labels_to_check_for_presence);


-- Prior load
create view spare_capacity_per_node as
select node_info.name as name,
       (node_info.cpu_allocatable - sum(pod_info.cpu_request)) as cpu_remaining,
       (node_info.memory_allocatable - sum(pod_info.memory_request)) as memory_remaining,
       (node_info.pods_allocatable) - sum(pod_info.pods_request) as pods_remaining
from node_info
join pod_info
     on pod_info.node_name = node_info.name and pod_info.node_name != 'null'
group by node_info.name, node_info.cpu_allocatable,
         node_info.memory_allocatable, node_info.pods_allocatable;

-- Pods per node from group limit predicate:
create view spare_pods_per_node_per_group as
select node_info.name as name,
       pods_per_node_per_group.owner_name as owner_name,
       (pods_per_node_per_group.pods_limit -
               (select count(pod_info.pod_name) from pod_info
                 where pod_info.node_name = node_info.name and pod_info.node_name != 'null'
                 and pod_info.owner_name = pods_per_node_per_group.owner_name)) as pods_remaining
from node_info, pods_per_node_per_group;

------------ Pod affinity candidates from already running nodes ---------
-- Valid nodes for pods based on pod affinity rules
create view candidate_nodes_for_pods_pod_affinity as
select distinct pod_labels.label_key, pod_labels.label_value, pod_info.node_name
from pod_info
join pod_labels
     on pod_info.pod_name = pod_labels.pod_name
join pod_affinity_match_expressions
     on pod_labels.label_key = pod_affinity_match_expressions.label_key
     and pod_labels.label_value = pod_affinity_match_expressions.label_value
where pod_info.node_name != 'null';

------------ Pod anti affinity candidates from already running nodes ---------
-- Valid nodes for pods based on pod anti affinity rules
create view blacklist_nodes_for_pods_pod_anti_affinity as
select distinct pod_labels.label_key, pod_labels.label_value, pod_info.node_name
from pod_info
join pod_labels
     on pod_info.pod_name = pod_labels.pod_name
join pod_anti_affinity_match_expressions
     on pod_labels.label_key = pod_anti_affinity_match_expressions.label_key
     and pod_labels.label_value = pod_anti_affinity_match_expressions.label_value
where pod_info.node_name != 'null';


-- Select all pods that need to be scheduled.
-- We also indicate boolean values to check whether
-- a pod has node selector or pod affinity labels,
-- and whether pod affinity rules already yields some subset of
-- nodes that we can assign pods to.
create view pods_to_assign_no_limit as
select
  pod_name,
  status,
  node_name as controllable__node_name,
  namespace,
  cpu_request,
  memory_request,
  ephemeral_storage_request,
  pods_request,
  owner_name,
  creation_timestamp,
  (CASE
      when pod_name in (select pod_name from pod_node_selector_labels) then true
        else false
      end) as has_node_affinity
from pod_info
where status = 'Pending' and node_name = 'null'
order by creation_timestamp;

-- This view is updated dynamically to change the limit. This
-- pattern is required because there is no clean way to enforce
-- a dynamic "LIMIT" clause.
create table batch_size
(
  pendingPodsLimit integer not null primary key
);

create view pods_to_assign as
select * from (
    select
         ROW_NUMBER() OVER () AS R,
         pods_to_assign_no_limit.*
       from pods_to_assign_no_limit
    ) as T
where T.R <= (select sum(pendingPodsLimit) from batch_size);

------------------------ Views for Affinity/AntiAffinity constraints -----------------------

-- Pods to assign joined by their labels. We also expose the node name variable column (which
-- we declare channeling constraints for later). This avoids joins in the solver code.
create view pods_to_assign_with_labels as
select distinct
       pod_labels.pod_name as pod_name,
       pod_labels.label_key as label_key,
       pod_labels.label_value as label_value,
       pods_to_assign.controllable__node_name as controllable__node_name_channeled
from pods_to_assign
join pod_labels
     on pods_to_assign.pod_name = pod_labels.pod_name;

------------- Node affinity -------------

-- Valid nodes for pods based on node affinity rules
create view candidate_nodes_for_pods as
select pods_to_assign.pod_name, node_labels.node_name
from pods_to_assign
join pod_node_selector_labels
     on pods_to_assign.pod_name = pod_node_selector_labels.pod_name
join node_labels
     on node_labels.label_key = pod_node_selector_labels.label_key
     and node_labels.label_value = pod_node_selector_labels.label_value;

-------------- Pod Affinity constraints -------------

-- Helper view that returns the set of pods with their affinity labels/rules
create view pods_to_assign_with_pod_affinity_labels as
select distinct
       pods_to_assign.pod_name as pod_name,
       pod_affinity_match_expressions.label_key as match_key,
       pod_affinity_match_expressions.label_value as match_value,
       pods_to_assign.controllable__node_name as controllable__node_name
from pods_to_assign
join pod_affinity_match_expressions
     on pods_to_assign.pod_name = pod_affinity_match_expressions.pod_name;

-------------- Pod Anti-Affinity constraints -------------

-- Helper view that returns the set of pods with their anti-affinity labels/rules
create view pods_to_assign_with_pod_anti_affinity_labels as
select distinct
       pods_to_assign.pod_name as pod_name,
       pod_anti_affinity_match_expressions.label_key as match_key,
       pod_anti_affinity_match_expressions.label_value as match_value,
       pods_to_assign.controllable__node_name as controllable__node_name
from pods_to_assign
join pod_anti_affinity_match_expressions
     on pods_to_assign.pod_name = pod_anti_affinity_match_expressions.pod_name;


-------------------- Preemption -------------------
-- Select all pods
create view all_pods as
select
  pod_name,
  status,
  node_name as current_node,
  node_name as controllable__node_name,
  namespace,
  cpu_request,
  memory_request,
  ephemeral_storage_request,
  pods_request,
  owner_name,
  creation_timestamp,
  priority,
  (CASE
      when pod_name in (select pod_name from pod_node_selector_labels) then true
        else false
      end) as has_node_affinity
from pod_info
order by creation_timestamp;