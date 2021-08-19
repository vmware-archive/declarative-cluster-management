create table node_info
(
  uid varchar(36) not null,
  name varchar(253) not null,
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
  pods_allocated bigint not null,
  primary key(uid)
);

create table pod_info
(
  uid varchar(36) not null,
  pod_name varchar(253) not null,
  status varchar(36) not null,
  node_name varchar(253) null,
  namespace varchar(253) not null,
  cpu_request bigint not null,
  memory_request bigint not null,
  ephemeral_storage_request bigint not null,
  pods_request bigint not null,
  owner_name varchar(100) not null,
  creation_timestamp varchar(100) not null,
  priority integer not null,
  scheduler_name varchar(50),
  has_node_selector_labels boolean not null,
  has_pod_affinity_requirements boolean not null,
  has_pod_anti_affinity_requirements boolean not null,
  has_node_port_requirements boolean not null,
  equivalence_class bigint not null,
  qos_class varchar(10) not null,
  resourceVersion bigint not null,
  last_requeue bigint not null,
  primary key(uid),
  constraint uc_namespaced_pod_name unique (pod_name, namespace)
);

create table match_expressions
(
  expr_id bigint not null,
  -- up to 253 for prefix, up to 63 for name and one for /
  label_key varchar(317) not null,
  label_operator varchar(30) not null,
  label_values varchar(63) array not null
  --primary key (label_key, label_operator, label_values) presto doesn't like composite keys
);

-- This table tracks the "ContainerPorts" fields of each pod.
-- It is used to enforce the PodFitsHostPorts constraint.
create table pod_ports_request
(
  pod_uid varchar(36) not null,
  host_ip varchar(100) not null,
  host_port integer not null,
  host_protocol varchar(10) not null
  --foreign key(pod_uid) references pod_info(uid) on delete cascade
);

-- This table tracks the set of hostports in use at each node.
-- Also used to enforce the PodFitsHostPorts constraint.
create table container_host_ports
(
  pod_uid varchar(36) not null,
  node_name varchar(253) not null,
  host_ip varchar(100) not null,
  host_port integer not null,
  host_protocol varchar(10) not null
  --foreign key(pod_uid) references pod_info(uid) on delete cascade,
  --foreign key(node_name) references node_info(name) on delete cascade
);

-- Tracks the set of node selector labels per pod.
create table pod_node_selector_labels
(
  pod_uid varchar(36) not null,
  term integer not null,
  match_expressions bigint array not null
  --foreign key(pod_uid) references pod_info(uid) on delete cascade
);

-- Tracks the set of pod affinity match expressions.
create table pod_affinity_match_expressions
(
  pod_uid varchar(36) not null,
  label_selector integer not null,
  match_expressions bigint array not null,
  topology_key varchar(100) not null
  --foreign key(pod_uid) references pod_info(uid) on delete cascade
);

-- Tracks the set of pod anti-affinity match expressions.
create table pod_anti_affinity_match_expressions
(
  pod_uid varchar(36) not null,
  label_selector integer not null,
  match_expressions bigint array not null,
  topology_key varchar(100) not null
  --foreign key(pod_uid) references pod_info(uid) on delete cascade
);


-- Tracks the set of labels per pod, and indicates if
-- any of them are also node selector labels
create table pod_labels
(
  pod_uid varchar(36) not null,
  label_key varchar(317) not null,
  label_value varchar(63) not null
  --foreign key(pod_uid) references pod_info(uid) on delete cascade,
  --primary key(pod_uid, label_key, label_value)
);

-- Tracks the set of labels per node
create table node_labels
(
  node_name varchar(253) not null,
  label_key varchar(317) not null,
  label_value varchar(63) not null
  --foreign key(node_name) references node_info(name) on delete cascade,
  --primary key(node_name, label_key, label_value)
);

-- Volume labels
create table volume_labels
(
  volume_name varchar(36) not null,
  pod_uid varchar(36) not null,
  label_key varchar(317) not null,
  label_value varchar(63) not null
  --foreign key(pod_uid) references pod_info(uid) on delete cascade
);

-- For pods that have ports exposed
create table pod_by_service
(
  pod_uid varchar(36) not null,
  service_name varchar(100) not null
  --foreign key(pod_uid) references pod_info(uid) on delete cascade
);

-- Service affinity labels
create table service_affinity_labels
(
  label_key varchar(317) not null
);


-- Labels present on node
create table labels_to_check_for_presence
(
  label_key varchar(317) not null,
  present boolean not null
);

-- Node taints
create table node_taints
(
  node_name varchar(253) not null,
  taint_key varchar(317) not null,
  taint_value varchar(63),
  taint_effect varchar(100) not null
  --foreign key(node_name) references node_info(name) on delete cascade
);

-- Pod taints.
create table pod_tolerations
(
  pod_uid varchar(36) not null,
  tolerations_key varchar(317),
  tolerations_value varchar(63),
  tolerations_effect varchar(100),
  tolerations_operator varchar(100)
  --foreign key(pod_uid) references pod_info(uid) on delete cascade
);

-- Tracks the set of node images that are already
-- available at a node
create table node_images
(
  node_name varchar(253) not null,
  image_name varchar(200) not null,
  image_size bigint not null
  --foreign key(node_name) references node_info(name) on delete cascade
);

-- Tracks the container images required by each pod
create table pod_images
(
  pod_uid varchar(36) not null,
  image_name varchar(200) not null
  --foreign key(pod_uid) references pod_info(uid) on delete cascade
);

-- Tracks pod disruption budget match expressions
create table pdb_match_expressions
(
  pdb_name varchar(30) not null,
  min_available integer not null,
  max_unavailable integer not null,
  allowed_disruptions integer not null
);


create index pod_info_idx on pod_info (status, node_name);
create index pod_node_selector_labels_fk_idx on pod_node_selector_labels (pod_uid);
create index node_labels_idx on node_labels (label_key, label_value);

create index pod_affinity_match_expressions_idx on pod_affinity_match_expressions (pod_uid);
create index pod_anti_affinity_match_expressions_idx on pod_anti_affinity_match_expressions (pod_uid);
create index pod_labels_idx on pod_labels (label_key, label_value);

create index match_expressions_idx on match_expressions (label_key, label_operator, label_values);