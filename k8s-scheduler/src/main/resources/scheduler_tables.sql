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
  priority integer not null,
  pod_num_selector_labels integer not null -- auxiliary state used for node selector query
);

-- This table tracks the "ContainerPorts" fields of each pod.
-- It is used to enforce the PodFitsHostPorts constraint.
create table pod_ports_request
(
  pod_name varchar(100) not null,
  host_ip varchar(100) not null,
  host_port integer not null,
  host_protocol varchar(10) not null,
  foreign key(pod_name) references pod_info(pod_name) on delete cascade
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
  foreign key(pod_name) references pod_info(pod_name) on delete cascade,
  foreign key(node_name) references node_info(name) on delete cascade
);

-- Tracks the set of node selector labels per pod.
create table pod_node_selector_labels
(
  pod_name varchar(100) not null,
  label_key varchar(100) not null,
  label_value varchar(36) not null,
  foreign key(pod_name) references pod_info(pod_name) on delete cascade
);

-- Tracks the set of pod affinity match expressions.
create table pod_affinity_match_expressions
(
  pod_name varchar(100) not null,
  label_key varchar(100) not null,
  label_value varchar(36) not null,
  operator varchar(30) not null,
  topology_key varchar(100) not null,
  foreign key(pod_name) references pod_info(pod_name) on delete cascade
);

-- Tracks the set of pod anti-affinity match expressions.
create table pod_anti_affinity_match_expressions
(
  pod_name varchar(100) not null,
  label_key varchar(100) not null,
  label_value varchar(36) not null,
  operator varchar(30) not null,
  topology_key varchar(100) not null,
  foreign key(pod_name) references pod_info(pod_name) on delete cascade
);


-- Tracks the set of labels per pod, and indicates if
-- any of them are also node selector labels
create table pod_labels
(
  pod_name varchar(100) not null,
  label_key varchar(100) not null,
  label_value varchar(36) not null,
  is_selector boolean not null,
  foreign key(pod_name) references pod_info(pod_name) on delete cascade
);

-- Tracks the set of labels per node
create table node_labels
(
  node_name varchar(36) not null,
  label_key varchar(100) not null,
  label_value varchar(36) not null,
  foreign key(node_name) references node_info(name) on delete cascade
);

-- Volume labels
create table volume_labels
(
  volume_name varchar(36) not null,
  pod_name varchar(100) not null,
  label_key varchar(100) not null,
  label_value varchar(36) not null,
  foreign key(pod_name) references pod_info(pod_name) on delete cascade
);

-- For pods that have ports exposed
create table pod_by_service
(
  pod_name varchar(100) not null,
  service_name varchar(100) not null,
  foreign key(pod_name) references pod_info(pod_name) on delete cascade
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
  foreign key(node_name) references node_info(name) on delete cascade
);

-- Pod taints.
create table pod_tolerations
(
  pod_name varchar(100) not null,
  taint_key varchar(100) not null,
  taint_value varchar(100) null,
  taint_effect varchar(100) not null,
  taint_operator varchar(100) not null,
  foreign key(pod_name) references pod_info(pod_name) on delete cascade
);

-- Tracks the set of node images that are already
-- available at a node
create table node_images
(
  node_name varchar(36) not null,
  image_name varchar(200) not null,
  image_size bigint not null,
  foreign key(node_name) references node_info(name) on delete cascade
);

-- Tracks the container images required by each pod
create table pod_images
(
  pod_name varchar(100) not null,
  image_name varchar(200) not null,
  foreign key(pod_name) references pod_info(pod_name) on delete cascade
);

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
  pod_num_selector_labels
from pod_info
where status = 'Pending' and node_name is null
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

-- Pods with port requests
create view pods_with_port_requests as
select pods_to_assign.controllable__node_name as controllable__node_name,
       pod_ports_request.host_port as host_port,
       pod_ports_request.host_ip as host_ip,
       pod_ports_request.host_protocol as host_protocol
from pods_to_assign
join pod_ports_request
     on pod_ports_request.pod_name = pods_to_assign.pod_name;

-- Pod node selectors
create view pod_node_selector_matches as
select pods_to_assign.pod_name as pod_name,
       node_labels.node_name as node_name
from pods_to_assign
join pod_node_selector_labels
     on pods_to_assign.pod_name = pod_node_selector_labels.pod_name
join node_labels
     on    pod_node_selector_labels.label_key = node_labels.label_key
       and pod_node_selector_labels.label_value = node_labels.label_value
group by pods_to_assign.pod_name, node_labels.node_name, pods_to_assign.pod_num_selector_labels
having count(*) = pods_to_assign.pod_num_selector_labels;

create index pod_name_idx1 on pod_info (pod_name, status, node_name);
create index pod_name_idx2 on pod_node_selector_labels (pod_name);
create index pod_name_idx3 on pod_node_selector_labels (label_key, label_value);
create index pod_name_idx4 on node_labels (label_key, label_value);
