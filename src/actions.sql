CREATE TABLE @extschema@.action_goals (
    goal_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    action_name text NOT NULL,
    action_type text NOT NULL,
    goal jsonb NOT NULL,
    desired_state text NOT NULL DEFAULT 'run' CHECK (desired_state IN ('run', 'cancel')),
    dispatch_state text NOT NULL DEFAULT 'pending'
        CHECK (dispatch_state IN ('pending', 'uncertain', 'accepted', 'rejected', 'canceled')),
    observed_state text NOT NULL DEFAULT 'unknown'
        CHECK (observed_state IN ('unknown', 'accepted', 'executing', 'canceling', 'succeeded', 'canceled', 'aborted')),
    feedback jsonb,
    result jsonb,
    cancel_response text,
    last_error text,
    submitted_by name NOT NULL DEFAULT session_user,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    completed_at timestamptz
);
CREATE INDEX action_goals_unfinished ON @extschema@.action_goals (created_at, goal_id)
    WHERE completed_at IS NULL;

-- These functions are the only supported writers other than the bridge.
-- EXECUTE authorizes control of all goals, so grant only to trusted operators.
CREATE FUNCTION @extschema@.send_goal(action_name text, action_type text, goal jsonb)
RETURNS uuid LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $body$
DECLARE id uuid;
BEGIN
    IF current_database() IS DISTINCT FROM current_setting('pg_ros2.database', true) THEN
        RAISE EXCEPTION 'send_goal requires pg_ros2 preloaded and installed in pg_ros2.database';
    END IF;
    IF action_name IS NULL OR action_name !~ '^(/[A-Za-z_][A-Za-z_0-9]*)+$'
       OR octet_length(action_name) > 200 THEN
        RAISE EXCEPTION 'action name must be a fully qualified ROS name of at most 200 bytes';
    END IF;
    IF action_type IS DISTINCT FROM 'example_interfaces/action/Fibonacci' THEN
        RAISE EXCEPTION 'unsupported action type: %; supported: example_interfaces/action/Fibonacci', action_type;
    END IF;
    IF goal IS NULL OR jsonb_typeof(goal) <> 'object' OR NOT goal ? 'order'
       OR goal - 'order' <> '{}'::jsonb OR jsonb_typeof(goal->'order') <> 'number'
       OR (goal->>'order') !~ '^-?[0-9]+$' THEN
        RAISE EXCEPTION 'Fibonacci goal must be an object containing only integer order';
    END IF;
    -- Reject out-of-range input before queuing an external operation.
    PERFORM (goal->>'order')::integer;
    INSERT INTO @extschema@.action_goals (action_name, action_type, goal)
        VALUES (action_name, action_type, goal) RETURNING goal_id INTO id;
    RETURN id;
END
$body$;

CREATE FUNCTION @extschema@.cancel_goal(id uuid)
RETURNS boolean LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog AS $body$
    WITH changed AS (
        UPDATE @extschema@.action_goals SET desired_state = 'cancel', updated_at = statement_timestamp(),
            dispatch_state = CASE WHEN dispatch_state = 'pending' THEN 'canceled' ELSE dispatch_state END,
            completed_at = CASE WHEN dispatch_state = 'pending' THEN statement_timestamp() ELSE completed_at END
        WHERE goal_id = id AND completed_at IS NULL RETURNING 1
    ) SELECT EXISTS (SELECT FROM changed)
$body$;

REVOKE ALL ON FUNCTION @extschema@.send_goal(text, text, jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION @extschema@.cancel_goal(uuid) FROM PUBLIC;
