-- Create the Employees table
CREATE TABLE employees (
    employee_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    position VARCHAR(50),
    salary NUMERIC(15, 2)
);

-- The function now sends a notification instead of writing to a table.
CREATE OR REPLACE FUNCTION notify_employees_changes()
RETURNS TRIGGER AS $$
DECLARE
    notification JSON;
BEGIN
    -- Determine the operation type
    IF (TG_OP = 'DELETE') THEN
        -- For DELETE, use the OLD data's ID
        notification = json_build_object(
                          'operation', TG_OP,
                          'case_id', OLD.employee_id);
    ELSE
        -- For INSERT or UPDATE, use the NEW data's ID
        notification = json_build_object(
                          'operation', TG_OP,
                          'employee_id', NEW.employee_id);
    END IF;

    -- Send the notification on the 'case_changes' channel.
    -- The second argument is the payload string.
    PERFORM pg_notify('employee_changes', notification::text);

    -- This trigger does not modify data, so we can return NULL.
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Create the new trigger to call the notify function
CREATE TRIGGER employee_changes_notify_trigger
AFTER INSERT OR UPDATE OR DELETE ON employees
FOR EACH ROW EXECUTE FUNCTION notify_employees_changes();

CREATE USER engineer WITH PASSWORD 'engie_pass';
GRANT CONNECT ON DATABASE cdc_db TO engineer;