create table node_info
(
  name varchar(36) not null primary key,
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
  schedulerName varchar(50),
  has_node_selector_labels boolean not null,
  has_pod_affinity_requirements boolean not null
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
  term integer not null,
  match_expression integer not null,
  num_match_expressions integer not null,
  label_key varchar(100) not null,
  label_operator varchar(12) not null,
  label_value varchar(36) null,
  foreign key(pod_name) references pod_info(pod_name) on delete cascade
);

-- Tracks the set of pod affinity match expressions.
create table pod_affinity_match_expressions
(
  pod_name varchar(100) not null,
  label_selector integer not null,
  match_expression integer not null,
  num_match_expressions integer not null,
  label_key varchar(100) not null,
  label_operator varchar(30) not null,
  label_value varchar(36) not null,
  topology_key varchar(100) not null,
  foreign key(pod_name) references pod_info(pod_name) on delete cascade
);

-- Tracks the set of pod anti-affinity match expressions.
create table pod_anti_affinity_match_expressions
(
  pod_name varchar(100) not null,
  label_key varchar(100) not null,
  label_operator varchar(12) not null,
  label_value varchar(36) not null,
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
  tolerations_key varchar(100),
  tolerations_value varchar(100),
  tolerations_effect varchar(100),
  tolerations_operator varchar(100),
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
  has_node_selector_labels,
  has_pod_affinity_requirements
from pod_info
where status = 'Pending' and node_name is null and schedulerName = 'dcm-scheduler'
order by creation_timestamp;

-- This view is updated dynamically to change the limit. This
-- pattern is required because there is no clean way to enforce
-- a dynamic "LIMIT" clause.
create table batch_size
(
  pendingPodsLimit integer not null primary key
);

create view pods_to_assign as
select * from pods_to_assign_no_limit limit 100;


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
        on
           (pod_node_selector_labels.label_operator = 'In'
            and pod_node_selector_labels.label_key = node_labels.label_key
            and pod_node_selector_labels.label_value = node_labels.label_value)
        or (pod_node_selector_labels.label_operator = 'Exists'
            and pod_node_selector_labels.label_key = node_labels.label_key)
        or (pod_node_selector_labels.label_operator = 'NotIn')
        or (pod_node_selector_labels.label_operator = 'DoesNotExist')
group by pods_to_assign.pod_name,  node_labels.node_name, pod_node_selector_labels.term,
         pod_node_selector_labels.label_operator, pod_node_selector_labels.num_match_expressions
having case pod_node_selector_labels.label_operator
            when 'NotIn'
                 then not(any(pod_node_selector_labels.label_key = node_labels.label_key
                              and pod_node_selector_labels.label_value = node_labels.label_value))
            when 'DoesNotExist'
                 then not(any(pod_node_selector_labels.label_key = node_labels.label_key))
            else count(distinct match_expression) = pod_node_selector_labels.num_match_expressions
       end;


create index pod_info_idx on pod_info (status, node_name);
create index pod_node_selector_labels_fk_idx on pod_node_selector_labels (pod_name);
create index node_labels_idx on node_labels (label_key, label_value);

-- Inter pod affinity
create view inter_pod_affinity_matches_inner as
select pods_to_assign.pod_name as pod_name,
       pod_labels.pod_name as matches,
       pod_info.node_name as node_name
from pods_to_assign
join pod_affinity_match_expressions
     on pods_to_assign.pod_name = pod_affinity_match_expressions.pod_name
join pod_labels
        on (pod_affinity_match_expressions.label_operator = 'In'
            and pod_affinity_match_expressions.label_key = pod_labels.label_key
            and pod_affinity_match_expressions.label_value = pod_labels.label_value)
        or (pod_affinity_match_expressions.label_operator = 'Exists'
            and pod_affinity_match_expressions.label_key = pod_labels.label_key)
        or (pod_affinity_match_expressions.label_operator = 'NotIn')
        or (pod_affinity_match_expressions.label_operator = 'DoesNotExist')
join pod_info
        on pod_labels.pod_name = pod_info.pod_name
group by pods_to_assign.pod_name,  pod_labels.pod_name, pod_affinity_match_expressions.label_selector,
         pod_affinity_match_expressions.topology_key, pod_affinity_match_expressions.label_operator,
         pod_affinity_match_expressions.num_match_expressions, pod_info.node_name
having case pod_affinity_match_expressions.label_operator
             when 'NotIn'
                  then not(any(pod_affinity_match_expressions.label_key = pod_labels.label_key
                               and pod_affinity_match_expressions.label_value = pod_labels.label_value))
             when 'DoesNotExist'
                  then not(any(pod_affinity_match_expressions.label_key = pod_labels.label_key))
             else count(distinct match_expression) = pod_affinity_match_expressions.num_match_expressions
       end;

create view inter_pod_affinity_matches as
select *, count(*) over (partition by pod_name) as num_matches from inter_pod_affinity_matches_inner;

create index pod_affinity_match_expressions_idx on pod_affinity_match_expressions (pod_name);
create index pod_labels_idx on pod_labels (label_key, label_value);


-- Spare capacity
create view spare_capacity_per_node as
select node_info.name as name,
       cast(node_info.cpu_allocatable - sum(pod_info.cpu_request) as integer) as cpu_remaining,
       cast(node_info.memory_allocatable - sum(pod_info.memory_request) as integer) as memory_remaining,
       cast(node_info.pods_allocatable - sum(pod_info.pods_request) as integer) as pods_remaining
from node_info
join pod_info
     on pod_info.node_name = node_info.name and pod_info.node_name != 'null'
group by node_info.name, node_info.cpu_allocatable,
         node_info.memory_allocatable, node_info.pods_allocatable;

-- Taints and tolerations
create view pods_that_tolerate_node_taints as
select pods_to_assign.pod_name as pod_name,
       A.node_name as node_name
from pods_to_assign
join pod_tolerations
     on pods_to_assign.pod_name = pod_tolerations.pod_name
join (select *, count(*) over (partition by node_name) as num_taints from node_taints) as A
     on pod_tolerations.tolerations_key = A.taint_key
     and (pod_tolerations.tolerations_effect = null
          or pod_tolerations.tolerations_effect = A.taint_effect)
     and (pod_tolerations.tolerations_operator = 'Exists'
          or pod_tolerations.tolerations_value = A.taint_value)
group by pod_tolerations.pod_name, A.node_name, A.num_taints
having count(*) = A.num_taints;

create view nodes_that_have_tolerations as
select distinct node_name from node_taints;