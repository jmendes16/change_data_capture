# change_data_capture
Demonstration using trigger-based change data capture using tools external to the database

Run docker compose build
docker compose up -d
log into pgadmin
add new server (db: cdc_db, host: postgres, username: postgres, password: postgres)
open up a query window
run 
INSERT INTO employees VALUES (1, 'alfie', 'bob', 'manager', 10),
	(2, 'charlie', 'dean', 'assistant', 5);

DELETE FROM employees WHERE employee_id = 1;
back in terminal on machine run python changelog.py
with terminal still visible, in pgadmin run
INSERT INTO employees VALUES (13, 'edward', 'fox', 'engineer', 15);
Ctrl + C in terminal
checkout employee_changes.log
docker compose down -v