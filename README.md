# inheritance-partitions-postgresql9.6
Procedure to inheritance-based partitioning to a existing table in PostgreSQL9.6

Partition the transactions table by month. You will need to use you’ll need to use inheritance-based partitioning.
: Provide an elegant implementation that 
(a) doesn’t neglect efficiency, scalability, and maintainability and 
(b) is fully confined within the database (no external dependencies).

1. Create a new master table, new_transactions.
create table new_transactions (
id int,
counter     integer,
dt_created  DATE DEFAULT CURRENT_DATE NOT NULL
);

2. Create children that inherit from master.

CREATE TABLE transactions_y2021m12 (
    CONSTRAINT pk_y2021m12 PRIMARY KEY (id),
    CONSTRAINT ck_y2021m12 CHECK ( dt_created >= DATE '2021-12-01' AND dt_created < DATE '2022-01-01'  )
) INHERITS (new_transactions);
CREATE INDEX idx_dt_created_y2021m12 ON transactions_y2021m12 (dt_created);

CREATE TABLE transactions_y2022m01 (
    CONSTRAINT pk_y2022m01 PRIMARY KEY (id),
    CONSTRAINT ck_y2022m01 CHECK ( dt_created >= DATE '2022-01-01' AND dt_created < DATE '2022-02-01'  )
) INHERITS (new_transactions);
CREATE INDEX idx_dt_created_y2022m01 ON transactions_y2022m01 (dt_created);

CREATE TABLE transactions_y2022m02 (
    CONSTRAINT pk_y2022m02 PRIMARY KEY (id),
    CONSTRAINT ck_y2022m02 CHECK ( dt_created >= DATE '2022-02-01' AND dt_created < DATE '2022-03-01'  )
) INHERITS (new_transactions);
CREATE INDEX idx_dt_created_y2022m02 ON transactions_y2022m02 (dt_created);

CREATE TABLE transactions_y2022m03 (
    CONSTRAINT pk_y2022m03 PRIMARY KEY (id),
    CONSTRAINT ck_y2022m03 CHECK ( dt_created >= DATE '2022-03-01' AND dt_created < DATE '2022-04-01'  )
) INHERITS (new_transactions);
CREATE INDEX idx_dt_created_y2022m03 ON transactions_y2022m03 (dt_created);


CREATE TABLE transactions_y2022m04 (
    CONSTRAINT pk_y2022m04 PRIMARY KEY (id),
    CONSTRAINT ck_y2022m04 CHECK ( dt_created >= DATE '2022-04-01' AND dt_created < DATE '2022-05-01'  )
) INHERITS (new_transactions);
CREATE INDEX idx_dt_created_y2022m04 ON transactions_y2022m04 (dt_created);


3. Copy historical data to the new_transaction table.

INSERT INTO transactions_y2021m12 (id,counter,dt_created)
SELECT id,counter,dt_created
from transactions
where dt_created >= '01/12/2021'::date AND dt_created < '01/01/2022'::date;

4. Stop insert/update on prod.
5. Copy most recent data to new master table

INSERT INTO transactions_y2022m01 (id,counter,dt_created)
SELECT id,counter,dt_created
from transactions
where dt_created >= '01/01/2022'::date AND dt_created < '01/02/2022'::date;

INSERT INTO transactions_y2022m02 (id,counter,dt_created)
SELECT id,counter,dt_created
from transactions
where dt_created >= '01/02/2022'::date AND dt_created < '01/03/2022'::date;

INSERT INTO transactions_y2022m04 (id,counter,dt_created)
SELECT id,counter,dt_created
from transactions
where dt_created >= '01/04/2022'::date AND dt_created < '01/05/2022'::date;


6. Rename tables so that new_master becomes the production database.

ALTER TABLE transactions RENAME TO transactions_backup;
ALTER TABLE new_transactions RENAME TO transactions;

7.Add function for INSERT statements to transactions table so that data gets passed to correct partition.

CREATE OR REPLACE FUNCTION fn_insert() RETURNS TRIGGER AS $$
BEGIN
    IF ( NEW.dt_created >= DATE '2021-12-01' AND
         NEW.dt_created < DATE '2022-01-01' ) THEN
        INSERT INTO transactions_y2021m12 VALUES (NEW.*);
    ELSIF ( NEW.dt_created >= DATE '2022-01-01' AND
         NEW.dt_created < DATE '2022-02-01' ) THEN
        INSERT INTO transactions_y2022m01 VALUES (NEW.*);
    ELSIF ( NEW.dt_created >= DATE '2022-02-01' AND
         NEW.dt_created < DATE '2022-03-01' ) THEN
        INSERT INTO transactions_y2022m02 VALUES (NEW.*);
    ELSIF ( NEW.dt_created >= DATE '2022-03-01' AND
         NEW.dt_created < DATE '2022-04-01' ) THEN
        INSERT INTO transactions_y2022m03 VALUES (NEW.*);
    ELSIF ( NEW.dt_created >= DATE '2022-04-01' AND
         NEW.dt_created < DATE '2022-05-01' ) THEN
        INSERT INTO transactions_y2022m04 VALUES (NEW.*);
    ELSE
        RAISE EXCEPTION 'Date out of range';
    END IF;
    RETURN NULL;
END;
$$

8. Add trigger so that function is called on INSERTS

CREATE TRIGGER tr_insert BEFORE INSERT ON transactions
FOR EACH ROW EXECUTE PROCEDURE fn_insert();

9. Set constraint exclusion to ON

SET constraint_exclusion = on;

10.Re-enable UPDATES and INSERTS on production database

12. drop table transactions_backup
LANGUAGE plpgsql;

 
########2.######
ALTER TABLE distributors ADD COLUMN working_address varchar(30) SET NULL;;
ALTER TABLE distributors ADD COLUMN passport_address varchar(30) SET NULL;;
ALTER TABLE distributors ADD COLUMN emergency_address varchar(30) SET NULL;;


4.
SELECT
 *
FROM
  scores
  where amount like ('%.00') and account_id=?
ORDER BY date DESC
LIMIT 10

explain plain select * from scores where amount like ('%.00') and account_id=? ORDER BY date DESC limit 10
create index concurrently "idx_amount"
on users using btree (amount);

create index concurrently "idx_account_id"
on users using btree (account_id);
