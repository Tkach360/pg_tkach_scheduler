/* pg_tkach_scheduler--1.0.sql */

\echo Use "CREATE EXTENSION pg_tkach_scheduler" to load this file. \quit

-- схема для расширения
CREATE SCHEMA ts;

-- функция для проверки наличия расширения в shared_preload_libraries
CREATE FUNCTION ts.check_shared_preload()
RETURNS BOOL
LANGUAGE C
AS 'MODULE_PATHNAME', 'ts_check_shared_preload';

-- как пользователю удобнее намекнуть, что нужно пользоваться
DO $$
DECLARE
    ts TEXT := 'ts';
BEGIN
    RAISE NOTICE 'Use name %, for example: ts.schedule_single()', quote_literal(ts);
    -- IF NOT ts.check_shared_preload() THEN
    --     RAISE EXCEPTION 
    --         'Need to add pg_tkach_scheduler to shared_preload_libraries';
    -- END IF;
END;
$$;

-- последовательность для создания id задачи
CREATE SEQUENCE ts.task_id_seq;

CREATE TYPE ts.TASK_TYPE AS ENUM ('single', 'repeat', 'repeat_limit', 'repeat_until');

-- таблица задач
-- денормализована специально, чтобы ускорить выборку данных
CREATE TABLE ts.task (

    -- идентификатор
    task_id BIGINT PRIMARY KEY DEFAULT pg_catalog.nextval('ts.task_id_seq'),
    
    -- SQL запрос, который будет выполняться по расписанию
    command TEXT NOT NULL,

    -- тип задачи, может быть только 'single', 'repeat', 'repeat limit %', 'repeat until '
    -- специально имеет тип TEXT, чтобы было понятнее при выводе
    type ts.TASK_TYPE NOT NULL,

    -- интервал выполнения задачи
    -- NULL для single задач
    exec_interval INTERVAL,

    -- время следующего выполнения
    time_next_exec TIMESTAMPTZ NOT NULL,

    -- лимит выполнения, сколько раз задача выполнится
    -- только для типа 'repeat limit'
    repeat_limit BIGINT,

    -- время, до которого должна выполняться задача
    -- только для типа 'repeat until'
    until TIMESTAMPTZ,

    -- комментарий к задаче
    note TEXT,

    -- пользователь, который запланировал задачу
    username TEXT NOT NULL DEFAULT current_user,

    -- база данных, над которой будет выполнятся задача
    database TEXT NOT NULL DEFAULT pg_catalog.current_database()
);

-- TODO: должен ли пользователь иметь полный доступ к этой таблице или нет?

-- запланировать задачу
CREATE FUNCTION ts.schedule(
    type ts.TASK_TYPE,
    command TEXT, 
    time_next_exec TIMESTAMPTZ,
    exec_interval INTERVAL DEFAULT NULL, 
    repeat_limit BIGINT DEFAULT NULL,
    until TIMESTAMPTZ DEFAULT NULL,
    note TEXT DEFAULT NULL
    -- username и database будут получены из кода на си
)
RETURNS BIGINT
LANGUAGE C
AS 'MODULE_PATHNAME', 'ts_schedule';
COMMENT ON FUNCTION ts.schedule(ts.TASK_TYPE,TEXT,TIMESTAMPTZ,INTERVAL,BIGINT,TIMESTAMPTZ,TEXT)
    IS 'schedule a pg_tkach_sheduler task, returns the task_id of the scheduled task';

-- запланировать одноразовую задачу
CREATE FUNCTION ts.schedule_single(
    command TEXT, 
    time_exec TIMESTAMPTZ,
    note TEXT DEFAULT NULL
)
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
BEGIN
        -- RAISE NOTICE 'Calling schedule with time_exec: %', time_exec;
    RETURN ts.schedule(
        'single'::ts.TASK_TYPE, 
        command, 
        time_exec, 
        NULL::INTERVAL, 
        NULL::BIGINT, 
        NULL::TIMESTAMPTZ, 
        note);
END;
$$;
COMMENT ON FUNCTION ts.schedule_single(TEXT,TIMESTAMPTZ,TEXT)
    IS 'schedule a pg_tkach_sheduler single task, returns the task_id of the scheduled task';

    -- RAISE NOTICE 'Calling schedule with time_exec: %', time_exec;
    -- SELECT ts.schedule(
    --     'single'::ts.TASK_TYPE, 
    --     command, 
    --     time_exec, 
    --     NULL::INTERVAL, 
    --     NULL::BIGINT, 
    --     NULL::TIMESTAMPTZ, 
    --     note) 
    -- INTO task_id;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'schedule' AND pronamespace = 'ts'::regnamespace) THEN
        RAISE EXCEPTION 'Function ts.schedule not found!';
    END IF;
END;
$$;