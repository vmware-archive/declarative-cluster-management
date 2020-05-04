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
  pods_allocatable bigint not null,
  cpu_allocated bigint not null,
  memory_allocated bigint not null,
  ephemeral_storage_allocated bigint not null,
  pods_allocated bigint not null
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
  has_pod_affinity_requirements boolean not null,
  has_pod_anti_affinity_requirements boolean not null,
  equivalence_class bigint not null,
  qos_class varchar(10) not null
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
  label_selector integer not null,
  match_expression integer not null,
  num_match_expressions integer not null,
  label_key varchar(100) not null,
  label_operator varchar(30) not null,
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
  has_pod_affinity_requirements,
  has_pod_anti_affinity_requirements,
  equivalence_class,
  qos_class
from pod_info
where status = 'Pending' and node_name is null and schedulerName = 'dcm-scheduler';

-- This view is updated dynamically to change the limit. This
-- pattern is required because there is no clean way to enforce
-- a dynamic "LIMIT" clause.
create table batch_size
(
  pendingPodsLimit integer not null primary key
);

create view pods_to_assign as
select * from pods_to_assign_no_limit limit 50;


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
where pods_to_assign.has_node_selector_labels = true
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

CREATE VIEW INTER_POD_AFFINITY_MATCHES_INNER AS
SELECT
  pods_to_assign.pod_name AS pod_name,
  pod_labels.pod_name AS matches,
  pod_info.node_name AS node_name
FROM
  pods_to_assign
  JOIN pod_affinity_match_expressions ON pods_to_assign.pod_name = pod_affinity_match_expressions.pod_name
  JOIN pod_labels ON (
    pod_affinity_match_expressions.label_operator = 'In'
    AND pod_affinity_match_expressions.label_key = pod_labels.label_key
    AND pod_affinity_match_expressions.label_value = pod_labels.label_value
  ) or (pod_affinity_match_expressions.label_operator = 'Exists'
            and pod_affinity_match_expressions.label_key = pod_labels.label_key)
  JOIN pod_info ON pod_labels.pod_name = pod_info.pod_name
  where pods_to_assign.has_pod_affinity_requirements = true
GROUP BY
  pods_to_assign.pod_name,
  pod_labels.pod_name,
  pod_affinity_match_expressions.label_selector,
  pod_affinity_match_expressions.topology_key,
  pod_affinity_match_expressions.label_operator,
  pod_affinity_match_expressions.num_match_expressions,
  pod_info.node_name
HAVING
  Count(DISTINCT match_expression) = pod_affinity_match_expressions.num_match_expressions;

-- Inter pod affinity (new)
-- CREATE VIEW interpodaffinitymatchesinner_Exists AS
-- SELECT
--   pods_to_assign.pod_name as podstoassign_podname,
--   pod_labels.pod_name as podlabels_podname,
--   pod_affinity_match_expressions.label_selector,
--   pod_affinity_match_expressions.topology_key,
--   pod_affinity_match_expressions.label_operator,
--   pod_affinity_match_expressions.num_match_expressions,
--   pod_affinity_match_expressions.match_expression,
--   pod_info.node_name
-- FROM
--   pods_to_assign
--   JOIN pod_affinity_match_expressions ON pods_to_assign.pod_name = pod_affinity_match_expressions.pod_name
--   JOIN pod_labels ON (
--     pod_affinity_match_expressions.label_operator = 'Exists'
--     and pod_affinity_match_expressions.label_key = pod_labels.label_key
--   )
--   JOIN pod_info ON pod_labels.pod_name = pod_info.pod_name
--   where pods_to_assign.has_pod_affinity_requirements = true;
--
-- CREATE VIEW interpodaffinitymatchesinner_In AS
-- SELECT
--   pods_to_assign.pod_name as podstoassign_podname,
--   pod_labels.pod_name as podlabels_podname,
--   pod_affinity_match_expressions.label_selector,
--   pod_affinity_match_expressions.topology_key,
--   pod_affinity_match_expressions.label_operator,
--   pod_affinity_match_expressions.num_match_expressions,
--   pod_affinity_match_expressions.match_expression,
--   pod_info.node_name
-- FROM
--   pods_to_assign
--   JOIN pod_affinity_match_expressions ON pods_to_assign.pod_name = pod_affinity_match_expressions.pod_name
--   JOIN pod_labels ON (
--     pod_affinity_match_expressions.label_operator = 'In'
--     and pod_affinity_match_expressions.label_key = pod_labels.label_key
--     and pod_affinity_match_expressions.label_value = pod_labels.label_value
--   )
--   JOIN pod_info ON pod_labels.pod_name = pod_info.pod_name
--   where pods_to_assign.has_pod_affinity_requirements = true;
--
-- CREATE VIEW INTER_POD_AFFINITY_MATCHES_INNER as
-- select
--   podstoassign_podname AS pod_name,
--   podlabels_podname AS matches,
--   node_name AS node_name
-- from
--   ((select * from interpodaffinitymatchesinner_In)
--     union
--       (select * from interpodaffinitymatchesinner_Exists))
-- GROUP BY
--   podstoassign_podname,
--   podlabels_podname,
--   label_selector,
--   topology_key,
--   label_operator,
--   num_match_expressions,
--   node_name
-- HAVING
--   Count(DISTINCT match_expression) = num_match_expressions;

-- create view inter_pod_affinity_matches_inner as
-- select pods_to_assign.pod_name as pod_name,
--        pod_labels.pod_name as matches,
--        pod_info.node_name as node_name
-- from pods_to_assign
-- join pod_affinity_match_expressions
--      on pods_to_assign.pod_name = pod_affinity_match_expressions.pod_name
-- join pod_labels
--         on (pod_affinity_match_expressions.label_operator = 'In'
--             and pod_affinity_match_expressions.label_key = pod_labels.label_key
--             and pod_affinity_match_expressions.label_value = pod_labels.label_value)
--         or (pod_affinity_match_expressions.label_operator = 'Exists'
--             and pod_affinity_match_expressions.label_key = pod_labels.label_key)
--         or (pod_affinity_match_expressions.label_operator = 'NotIn')
--         or (pod_affinity_match_expressions.label_operator = 'DoesNotExist')
-- join pod_info
--         on pod_labels.pod_name = pod_info.pod_name
-- where pods_to_assign.has_pod_affinity_requirements = true
-- group by pods_to_assign.pod_name,  pod_labels.pod_name, pod_affinity_match_expressions.label_selector,
--          pod_affinity_match_expressions.topology_key, pod_affinity_match_expressions.label_operator,
--          pod_affinity_match_expressions.num_match_expressions, pod_info.node_name
-- having case pod_affinity_match_expressions.label_operator
--              when 'NotIn'
--                   then not(any(pod_affinity_match_expressions.label_key = pod_labels.label_key
--                                and pod_affinity_match_expressions.label_value = pod_labels.label_value))
--              when 'DoesNotExist'
--                   then not(any(pod_affinity_match_expressions.label_key = pod_labels.label_key))
--              else count(distinct match_expression) = pod_affinity_match_expressions.num_match_expressions
--        end;

create view inter_pod_affinity_matches as
select *, count(*) over (partition by pod_name) as num_matches from inter_pod_affinity_matches_inner;

create index pod_affinity_match_expressions_idx on pod_affinity_match_expressions (pod_name);
create index pod_labels_idx on pod_labels (label_key, label_value);


-- Inter pod anti-affinity
-- TODO: the having clause could be simplified: if even a single term matches, we can preclude a node
create view inter_pod_anti_affinity_matches_inner as
select pods_to_assign.pod_name as pod_name,
       pod_labels.pod_name as matches,
       pod_info.node_name as node_name
from pods_to_assign
join pod_anti_affinity_match_expressions
     on pods_to_assign.pod_name = pod_anti_affinity_match_expressions.pod_name
join pod_labels
        on (pod_anti_affinity_match_expressions.label_operator = 'In'
            and pod_anti_affinity_match_expressions.label_key = pod_labels.label_key
            and pod_anti_affinity_match_expressions.label_value = pod_labels.label_value)
        or (pod_anti_affinity_match_expressions.label_operator = 'Exists'
            and pod_anti_affinity_match_expressions.label_key = pod_labels.label_key)
        or (pod_anti_affinity_match_expressions.label_operator = 'NotIn')
        or (pod_anti_affinity_match_expressions.label_operator = 'DoesNotExist')
join pod_info
        on pod_labels.pod_name = pod_info.pod_name
where pods_to_assign.has_pod_anti_affinity_requirements = true
group by pods_to_assign.pod_name,  pod_labels.pod_name, pod_anti_affinity_match_expressions.label_selector,
         pod_anti_affinity_match_expressions.topology_key, pod_anti_affinity_match_expressions.label_operator,
         pod_anti_affinity_match_expressions.num_match_expressions, pod_info.node_name
having case pod_anti_affinity_match_expressions.label_operator
             when 'NotIn'
                  then not(any(pod_anti_affinity_match_expressions.label_key = pod_labels.label_key
                               and pod_anti_affinity_match_expressions.label_value = pod_labels.label_value))
             when 'DoesNotExist'
                  then not(any(pod_anti_affinity_match_expressions.label_key = pod_labels.label_key))
             else count(distinct match_expression) = pod_anti_affinity_match_expressions.num_match_expressions
       end;

create view inter_pod_anti_affinity_matches as
select *, count(*) over (partition by pod_name) as num_matches from inter_pod_anti_affinity_matches_inner;

-- Spare capacity
create view spare_capacity_per_node as
select name as name,
  cpu_allocatable - cpu_allocated as cpu_remaining,
  memory_allocatable - memory_allocated as memory_remaining,
  pods_allocatable - pods_allocated as pods_remaining
from node_info
where unschedulable = false and
      memory_pressure = false and
      out_of_disk = false and
      disk_pressure = false and
      pid_pressure = false and
      network_unavailable = false and
      ready = true and
      cpu_allocated < cpu_allocatable and
      memory_allocated <  memory_allocatable and
      pods_allocated < pods_allocatable;

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

-- Avoid overloaded nodes or nodes that report being under resource pressure
create view allowed_nodes as
select name
from spare_capacity_per_node;