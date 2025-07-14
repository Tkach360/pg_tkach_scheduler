-- sql/pg_tkach_scheduler-test.sql

CREATE EXTENSION IF NOT EXISTS pg_tkach_scheduler;

SELECT * FROM ts.task;

DROP EXTENSION pg_tkach_scheduler;