--
-- MULTI_TASK_ASSIGNMENT
--


SET citus.next_shard_id TO 880000;

-- print whether we're using version > 9 to make version-specific tests clear
SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int > 9 AS version_above_nine;

-- the function simply parses the results and returns 'shardId@worker'
-- for all the explain task outputs
CREATE OR REPLACE FUNCTION parse_explain_output(in qry text, in table_name text, out r text) 
RETURNS SETOF TEXT AS $$
DECLARE
       portOfTheTask text;
       shardOfTheTask text;
begin
  for r in execute qry loop
    IF r LIKE '%port%' THEN
      portOfTheTask = substring(r, '([0-9]{1,10})');
    END IF;

    IF r LIKE '%' || table_name || '%' THEN
      shardOfTheTask = substring(r, '([0-9]{5,10})');
    END IF;

  end loop;
  return QUERY SELECT shardOfTheTask || '@' || portOfTheTask;
end; $$ language plpgsql;


SET citus.explain_distributed_queries TO off;


-- Check that our policies for assigning tasks to worker nodes run as expected.
-- To test this, we first create a shell table, and then manually insert shard
-- and shard placement data into system catalogs. We next run Explain command,
-- and check that tasks are assigned to worker nodes as expected.

CREATE TABLE task_assignment_test_table (test_id integer);
SELECT create_distributed_table('task_assignment_test_table', 'test_id', 'append');

-- Create logical shards with shardids 200, 201, and 202

INSERT INTO pg_dist_shard (logicalrelid, shardid, shardstorage, shardminvalue, shardmaxvalue)
	SELECT pg_class.oid, series.index, 'r', 1, 1000
	FROM pg_class, generate_series(200, 202) AS series(index)
	WHERE pg_class.relname = 'task_assignment_test_table';

-- Create shard placements for shard 200 and 201

INSERT INTO pg_dist_shard_placement (shardid, shardstate, shardlength, nodename, nodeport)
       SELECT 200, 1, 1, nodename, nodeport
       FROM pg_dist_shard_placement
       GROUP BY nodename, nodeport
       ORDER BY nodename, nodeport ASC
       LIMIT 2;

INSERT INTO pg_dist_shard_placement (shardid, shardstate, shardlength, nodename, nodeport)
       SELECT 201, 1, 1, nodename, nodeport
       FROM pg_dist_shard_placement
       GROUP BY nodename, nodeport
       ORDER BY nodename, nodeport ASC
       LIMIT 2;

-- Create shard placements for shard 202

INSERT INTO pg_dist_shard_placement (shardid, shardstate, shardlength, nodename, nodeport)
       SELECT 202, 1, 1, nodename, nodeport
       FROM pg_dist_shard_placement
       GROUP BY nodename, nodeport
       ORDER BY nodename, nodeport DESC
       LIMIT 2;

-- Start transaction block to avoid auto commits. This avoids additional debug
-- messages from getting printed at real transaction starts and commits.

BEGIN;

-- Increase log level to see which worker nodes tasks are assigned to. Note that
-- the following log messages print node name and port numbers; and node numbers
-- in regression tests depend upon PG_VERSION_NUM.

SET client_min_messages TO DEBUG3;

-- First test the default greedy task assignment policy

SET citus.task_assignment_policy TO 'greedy';

EXPLAIN SELECT count(*) FROM task_assignment_test_table;

EXPLAIN SELECT count(*) FROM task_assignment_test_table;

-- Next test the first-replica task assignment policy

SET citus.task_assignment_policy TO 'first-replica';

EXPLAIN SELECT count(*) FROM task_assignment_test_table;

EXPLAIN SELECT count(*) FROM task_assignment_test_table;

COMMIT;



CREATE TABLE task_assignment_reference_table (test_id  integer);
SELECT create_reference_table('task_assignment_reference_table');

BEGIN;

SET LOCAL client_min_messages TO DEBUG3;
SET LOCAL citus.explain_distributed_queries TO off;

-- Check how task_assignment_policy impact planning decisions for reference tables
SET LOCAL citus.task_assignment_policy TO 'greedy';
EXPLAIN (COSTS FALSE) SELECT * FROM task_assignment_reference_table;
EXPLAIN (COSTS FALSE) SELECT * FROM task_assignment_reference_table;

SET LOCAL citus.task_assignment_policy TO 'first-replica';
EXPLAIN (COSTS FALSE) SELECT * FROM task_assignment_reference_table;
EXPLAIN (COSTS FALSE) SELECT * FROM task_assignment_reference_table;

ROLLBACK;

RESET client_min_messages;


-- Now, lets test round-robin policy
-- round-robin policy relies on PostgreSQL's local transactionId, 
-- which might change and we don't have any control over it.
-- the important thing that we look for is that round-robin policy 
-- should give the same output for executions in the same transaction 
-- and different output for executions that are not inside the
-- same transaction. To ensure that, we define a helper function
BEGIN;

SET LOCAL citus.explain_distributed_queries TO on;

CREATE TEMPORARY TABLE explain_outputs (value text);
SET LOCAL citus.task_assignment_policy TO 'round-robin';

INSERT INTO explain_outputs 
       SELECT parse_explain_output('EXPLAIN SELECT count(*) FROM task_assignment_reference_table;', 'task_assignment_reference_table');
INSERT INTO explain_outputs 
       SELECT parse_explain_output('EXPLAIN SELECT count(*) FROM task_assignment_reference_table;', 'task_assignment_reference_table');

-- given that we're in the same transaction, the count should be 1
SELECT count(DISTINCT value) FROM explain_outputs;
TRUNCATE explain_outputs;
COMMIT;

-- now test round-robin policy outside
-- a transaction, we should see the assignments
-- change on every execution
SET citus.task_assignment_policy TO 'round-robin';
SET citus.explain_distributed_queries TO ON;

INSERT INTO explain_outputs 
       SELECT parse_explain_output('EXPLAIN SELECT count(*) FROM task_assignment_reference_table;', 'task_assignment_reference_table');
INSERT INTO explain_outputs 
       SELECT parse_explain_output('EXPLAIN SELECT count(*) FROM task_assignment_reference_table;', 'task_assignment_reference_table');

-- given that we're in the same transaction, the count should be 2
-- since there are two different worker nodes
SELECT count(DISTINCT value) FROM explain_outputs;
TRUNCATE explain_outputs;

-- same test with a distributed table
-- we keep this test because as of this commit, the code
-- paths for reference tables and distributed tables are 
-- not the same
SET citus.shard_replication_factor TO 2;

CREATE TABLE task_assignment_replicated_hash (test_id  integer);
SELECT create_distributed_table('task_assignment_replicated_hash', 'test_id');

BEGIN;

SET LOCAL citus.explain_distributed_queries TO on;
SET LOCAL citus.task_assignment_policy TO 'round-robin';

INSERT INTO explain_outputs 
       SELECT parse_explain_output('EXPLAIN SELECT count(*) FROM task_assignment_replicated_hash;', 'task_assignment_replicated_hash');
INSERT INTO explain_outputs 
       SELECT parse_explain_output('EXPLAIN SELECT count(*) FROM task_assignment_replicated_hash;', 'task_assignment_replicated_hash');

-- given that we're in the same transaction, the count should be 1
SELECT count(DISTINCT value) FROM explain_outputs;
TRUNCATE explain_outputs;
COMMIT;

-- now test round-robin policy outside
-- a transaction, we should see the assignments
-- change on every execution
SET citus.task_assignment_policy TO 'round-robin';
SET citus.explain_distributed_queries TO ON;

INSERT INTO explain_outputs 
       SELECT parse_explain_output('EXPLAIN SELECT count(*) FROM task_assignment_replicated_hash;', 'task_assignment_replicated_hash');
INSERT INTO explain_outputs 
       SELECT parse_explain_output('EXPLAIN SELECT count(*) FROM task_assignment_replicated_hash;', 'task_assignment_replicated_hash');

-- given that we're in the same transaction, the count should be 2
-- since there are two different worker nodes
SELECT count(DISTINCT value) FROM explain_outputs;
TRUNCATE explain_outputs;

-- test that the round robin policy detects the anchor shard correctly
-- we should not pick a reference table shard as the anchor shard when joining with a distributed table
SET citus.shard_replication_factor TO 1;

CREATE TABLE task_assignment_nonreplicated_hash (test_id  integer, ref_id integer);
SELECT create_distributed_table('task_assignment_nonreplicated_hash', 'test_id');

-- run the query two times to make sure that it hits the correct worker every time
INSERT INTO explain_outputs
SELECT parse_explain_output($cmd$
EXPLAIN SELECT *
FROM (SELECT * FROM task_assignment_nonreplicated_hash WHERE test_id = 3) AS dist
       LEFT JOIN task_assignment_reference_table ref
                 ON dist.ref_id = ref.test_id
$cmd$, 'task_assignment_nonreplicated_hash');

INSERT INTO explain_outputs
SELECT parse_explain_output($cmd$
EXPLAIN SELECT *
FROM (SELECT * FROM task_assignment_nonreplicated_hash WHERE test_id = 3) AS dist
       LEFT JOIN task_assignment_reference_table ref
                 ON dist.ref_id = ref.test_id
$cmd$, 'task_assignment_nonreplicated_hash');

-- The count should be 1 since the shard exists in only one worker node
SELECT count(DISTINCT value) FROM explain_outputs;
TRUNCATE explain_outputs;

-- We should be able to use round-robin with router queries that
-- only contains intermediate results
CREATE TABLE task_assignment_test_table_2 (test_id integer);
SELECT create_distributed_table('task_assignment_test_table_2', 'test_id');

SET citus.task_assignment_policy TO 'round-robin';

-- Run the query two times to make sure that it hits two different workers
-- on consecutive runs
INSERT INTO explain_outputs
SELECT parse_explain_output($cmd$
EXPLAIN WITH q1 AS (SELECT * FROM task_assignment_test_table_2) SELECT * FROM q1
$cmd$, 'task_assignment_test_table_2');

INSERT INTO explain_outputs
SELECT parse_explain_output($cmd$
EXPLAIN WITH q1 AS (SELECT * FROM task_assignment_test_table_2) SELECT * FROM q1
$cmd$, 'task_assignment_test_table_2');

-- The count should be 2 since the intermediate results are processed on
-- different workers
SELECT count(DISTINCT value) FROM explain_outputs;

RESET citus.task_assignment_policy;
RESET client_min_messages;

--  make sure we set the right anchor shardid for task
-- citus/issues/2749 , anchor_shardId should be assigned as dis_table'shardid 
SET citus.shard_replication_factor TO 1;
SET citus.replication_model TO streaming;
CREATE TABLE reference_table (id bigint, value bigint);
SELECT create_reference_table('reference_table');

CREATE TABLE dis_table (
        id bigint,
        min_value numeric,
        average_value numeric,
        count int,
        uniques int);

SELECT create_distributed_table('dis_table', 'id');

INSERT INTO reference_table VALUES (1, 100);
INSERT INTO dis_table VALUES (1);

--- thish should be ok, query will send to one worker
UPDATE dis_table SET average_value = average_query.average FROM (
        SELECT avg(value) AS average FROM reference_table WHERE id = 1
) average_query
WHERE id = 1;

--- this also should be ok
UPDATE reference_table SET value = average_query.average FROM (
        SELECT avg(value) AS average FROM reference_table WHERE id = 1
) average_query
WHERE id = 1;

--- this current not ok
UPDATE reference_table SET value = average_query.average FROM (
        SELECT average_value AS average FROM dis_table WHERE id = 1
) average_query
WHERE id = 1;

DROP TABLE task_assignment_replicated_hash, task_assignment_nonreplicated_hash,
  task_assignment_reference_table, task_assignment_test_table_2, explain_outputs,
  dis_table,reference_table ;
