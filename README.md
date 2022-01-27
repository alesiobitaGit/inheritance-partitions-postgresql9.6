#Exercise 3
inheritance-partitions-postgresql9.6
Procedure to inheritance-based partitioning to a existing table in PostgreSQL9.6
Partition the transactions table by month, but note that declarative partitioning is not available in PostgreSQL 9.6 and you’ll need to use inheritance-based partitioning.
Hint: Provide an elegant implementation that 
(a) doesn’t neglect efficiency, scalability, and maintainability and 
(b) is fully confined within the database (no external dependencies).

-- With the below query we can find which date is the unix timestamp
select to_timestamp(1509038400991 / 1000)::date;

--Find the lastest date on the table:
select created from public.transactions order by created desc limit 10;
select to_timestamp(1523627312507 / 1000)::date;
--"2018-04-13"

--Find the oldest date on the table:
select created from public.transactions order by created asc limit 10;
select to_timestamp(1507859336914 / 1000)::date;
--1507859336914

1. Create a new master table, new_transactions.
	CREATE TABLE IF NOT EXISTS public.new_transactions
(
    id text COLLATE pg_catalog."default",
    card_id text COLLATE pg_catalog."default",
    account_id text COLLATE pg_catalog."default",
    amount text COLLATE pg_catalog."default",
    created text COLLATE pg_catalog."default"
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;
ALTER TABLE IF EXISTS public.new_transactions
    OWNER to jean_sammet;

2. Create children that inherit from master.

CREATE TABLE transactions_y2017m10 (
    CONSTRAINT pk_y2017m10 PRIMARY KEY (id),
    CONSTRAINT ck_y2017m10 CHECK ( to_timestamp(cast(created as bigint) / 1000)::date >= DATE '2017-10-01' and to_timestamp(cast(created as bigint) / 1000)::date < DATE '2017-11-01')
) INHERITS (new_transactions);
CREATE INDEX idx_transactions_y2017m10 ON transactions_y2017m10 (created);
ALTER TABLE IF EXISTS public.transactions_y2017m10 OWNER to jean_sammet;



CREATE TABLE transactions_y2017m11 (
    CONSTRAINT pk_y2017m11 PRIMARY KEY (id),
    CONSTRAINT ck_y2017m11 CHECK ( to_timestamp(cast(created as bigint) / 1000)::date >= DATE '2017-11-01' and to_timestamp(cast(created as bigint) / 1000)::date < DATE '2017-12-01' )
) INHERITS (new_transactions);
CREATE INDEX idx_transactions_y2017m11 ON transactions_y2017m11 (created);
ALTER TABLE IF EXISTS public.transactions_y2017m11 OWNER to jean_sammet;

CREATE TABLE transactions_y2017m12 (
    CONSTRAINT pk_y2017m12 PRIMARY KEY (id),
    CONSTRAINT ck_y2017m12 CHECK ( to_timestamp(cast(created as bigint) / 1000)::date >= DATE '2017-12-01' and to_timestamp(cast(created as bigint) / 1000)::date < DATE '2018-01-01' )
) INHERITS (new_transactions);
CREATE INDEX idx_transactions_y2017m12 ON transactions_y2017m12 (created);
ALTER TABLE IF EXISTS public.transactions_y2017m12 OWNER to jean_sammet;

CREATE TABLE transactions_y2018m01 (
    CONSTRAINT pk_y2018m01 PRIMARY KEY (id),
    CONSTRAINT ck_y2018m01 CHECK ( to_timestamp(cast(created as bigint) / 1000)::date >= DATE '2018-01-01' and to_timestamp(cast(created as bigint) / 1000)::date < DATE '2018-02-01' )
) INHERITS (new_transactions);
CREATE INDEX idx_transactions_y2018m01 ON transactions_y2018m01 (created);
ALTER TABLE IF EXISTS public.transactions_y2018m01 OWNER to jean_sammet;

CREATE TABLE transactions_y2018m02 (
    CONSTRAINT pk_y2018m02 PRIMARY KEY (id),
    CONSTRAINT ck_y2018m02 CHECK ( to_timestamp(cast(created as bigint) / 1000)::date >= DATE '2018-02-01' and to_timestamp(cast(created as bigint) / 1000)::date < DATE '2018-03-01' )
) INHERITS (new_transactions);
CREATE INDEX idx_transactions_y2018m02 ON transactions_y2018m02 (created);
ALTER TABLE IF EXISTS public.transactions_y2018m02 OWNER to jean_sammet;

CREATE TABLE transactions_y2018m03 (
    CONSTRAINT pk_y2018m03 PRIMARY KEY (id),
    CONSTRAINT ck_y2018m03 CHECK ( to_timestamp(cast(created as bigint) / 1000)::date >= DATE '2018-03-01' and to_timestamp(cast(created as bigint) / 1000)::date < DATE '2018-04-01'  )
) INHERITS (new_transactions);
CREATE INDEX idx_transactions_y2018m03 ON transactions_y2018m03 (created);
ALTER TABLE IF EXISTS public.transactions_y2018m03 OWNER to jean_sammet;


CREATE TABLE transactions_y2018m04 (
    CONSTRAINT pk_y2018m04 PRIMARY KEY (id),
    CONSTRAINT ck_y2018m04 CHECK ( to_timestamp(cast(created as bigint) / 1000)::date >= DATE '2018-04-01' and to_timestamp(cast(created as bigint) / 1000)::date < DATE '2018-05-01'  )
) INHERITS (new_transactions);
CREATE INDEX idx_transactions_y2018m04 ON transactions_y2018m04 (created);
ALTER TABLE IF EXISTS public.transactions_y2018m04 OWNER to jean_sammet;


CREATE TABLE transactions_y2018m05 (
    CONSTRAINT pk_y2018m05 PRIMARY KEY (id),
    CONSTRAINT ck_y2018m05 CHECK ( to_timestamp(cast(created as bigint) / 1000)::date >= DATE '2018-05-01' and to_timestamp(cast(created as bigint) / 1000)::date < DATE '2018-06-01'  )
) INHERITS (new_transactions);
CREATE INDEX idx_transactions_y2018m05 ON transactions_y2018m05 (created);
ALTER TABLE IF EXISTS public.transactions_y2018m05 OWNER to jean_sammet;


3. Copy historical data to the new_transaction table. Copy most recent data to new master table.
INSERT INTO transactions_y2017m10 (id,card_id,account_id,amount,created)
SELECT id,card_id,account_id,amount,created
from public.transactions
where to_timestamp(cast(created as bigint) / 1000)::date >= DATE '2017-10-01' and to_timestamp(cast(created as bigint) / 1000)::date < DATE '2017-11-01';


INSERT INTO transactions_y2017m11 (id,card_id,account_id,amount,created)
SELECT id,card_id,account_id,amount,created
from public.transactions
where to_timestamp(cast(created as bigint) / 1000)::date >= DATE '2017-11-01' and to_timestamp(cast(created as bigint) / 1000)::date < DATE '2017-12-01';

INSERT INTO transactions_y2017m12 (id,card_id,account_id,amount,created)
SELECT id,card_id,account_id,amount,created
from public.transactions
where to_timestamp(cast(created as bigint) / 1000)::date >= DATE '2017-12-01' and to_timestamp(cast(created as bigint) / 1000)::date < DATE '2018-01-01';

INSERT INTO transactions_y2018m01 (id,card_id,account_id,amount,created)
SELECT id,card_id,account_id,amount,created
from public.transactions
where to_timestamp(cast(created as bigint) / 1000)::date >= DATE '2018-01-01' and to_timestamp(cast(created as bigint) / 1000)::date < DATE '2018-02-01';

INSERT INTO transactions_y2018m02 (id,card_id,account_id,amount,created)
SELECT id,card_id,account_id,amount,created
from public.transactions
where to_timestamp(cast(created as bigint) / 1000)::date >= DATE '2018-02-01' and to_timestamp(cast(created as bigint) / 1000)::date < DATE '2018-03-01';

INSERT INTO transactions_y2018m03 (id,card_id,account_id,amount,created)
SELECT id,card_id,account_id,amount,created
from public.transactions
where to_timestamp(cast(created as bigint) / 1000)::date >= DATE '2018-03-01' and to_timestamp(cast(created as bigint) / 1000)::date < DATE '2018-04-01';


INSERT INTO transactions_y2018m04 (id,card_id,account_id,amount,created)
SELECT id,card_id,account_id,amount,created
from public.transactions
where to_timestamp(cast(created as bigint) / 1000)::date >= DATE '2018-04-01' and to_timestamp(cast(created as bigint) / 1000)::date < DATE '2018-05-01';


6. Rename tables so that new_master becomes the production database.

ALTER TABLE transactions RENAME TO transactions_backup2;
ALTER TABLE new_transactions RENAME TO transactions;

7.Add function for INSERT statements to transactions table so that data gets passed to correct partition.

CREATE OR REPLACE FUNCTION fn_insert() RETURNS TRIGGER AS $$
BEGIN
    IF ( to_timestamp(cast(NEW.created as bigint) / 1000) >= DATE '2017-10-01' AND
         to_timestamp(cast(NEW.created as bigint) / 1000) < DATE '2017-11-01') THEN
        INSERT INTO transactions_y2017m10 VALUES (NEW.*);
    ELSIF ( to_timestamp(cast(NEW.created as bigint) / 1000) >= DATE '2017-11-01' AND
         to_timestamp(cast(NEW.created as bigint) / 1000) < DATE '2017-12-01' ) THEN
        INSERT INTO transactions_y2017m11 VALUES (NEW.*);
    ELSIF ( to_timestamp(cast(NEW.created as bigint) / 1000) >= DATE '2017-12-01' AND
         to_timestamp(cast(NEW.created as bigint) / 1000) < DATE '2018-01-01' ) THEN
        INSERT INTO transactions_y2017m12 VALUES (NEW.*);		
    ELSIF ( to_timestamp(cast(NEW.created as bigint) / 1000) >= DATE '2018-01-01' AND
         to_timestamp(cast(NEW.created as bigint) / 1000) < DATE '2018-02-01' ) THEN
        INSERT INTO transactions_y2018m01 VALUES (NEW.*);		
    ELSIF ( to_timestamp(cast(NEW.created as bigint) / 1000) >= DATE '2018-02-01' AND
         to_timestamp(cast(NEW.created as bigint) / 1000) < DATE '2018-03-01' ) THEN
        INSERT INTO transactions_y2018m02 VALUES (NEW.*);		
    ELSIF ( to_timestamp(cast(NEW.created as bigint) / 1000) >= DATE '2018-03-01' AND
         to_timestamp(cast(NEW.created as bigint) / 1000) < DATE '2018-04-01' ) THEN
        INSERT INTO transactions_y2018m03 VALUES (NEW.*);
    ELSIF ( to_timestamp(cast(NEW.created as bigint) / 1000) >= DATE '2018-04-01' AND
         to_timestamp(cast(NEW.created as bigint) / 1000) < DATE '2018-05-01' ) THEN
        INSERT INTO transactions_y2018m04 VALUES (NEW.*);
    ELSIF ( to_timestamp(cast(NEW.created as bigint) / 1000) >= DATE '2018-05-01' AND
         to_timestamp(cast(NEW.created as bigint) / 1000) < DATE '2018-06-01' ) THEN
        INSERT INTO transactions_y2018m05 VALUES (NEW.*);		
    ELSE
        RAISE EXCEPTION 'Date out of range';
    END IF;
    RETURN NULL;
END;
$$
LANGUAGE plpgsql;


8. Add trigger so that function is called on INSERTS

CREATE TRIGGER tr_insert BEFORE INSERT ON transactions
FOR EACH ROW EXECUTE PROCEDURE fn_insert();

9. Set constraint exclusion to ON

SET constraint_exclusion = on;

10.Re-enable UPDATES and INSERTS on production database

12. drop table transactions_backup
LANGUAGE plpgsql;

 
#Exercise 2
Currently we store only the user shipping address. In addition to that, the user will soon be able to optionally set one working address, one passport address and one emergency address. Considering that even more optional address types may be introduced in the future, please make the necessary changes to the schema to model the new requirements as most appropriate.

Below are addded the new columns that the user can optionally set or not, it depends from the user prospected.

ALTER TABLE users ADD COLUMN working_address text COLLATE pg_catalog."default" ;
ALTER TABLE users ADD COLUMN passport_address text COLLATE pg_catalog."default";
ALTER TABLE users ADD COLUMN emergency_address text COLLATE pg_catalog."default";


-- Table: public.users

-- DROP TABLE IF EXISTS public.users;

CREATE TABLE IF NOT EXISTS public.users_optionally_addr
(
    user_id text COLLATE pg_catalog."default",
    optianally_address_id text COLLATE pg_catalog."default"
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.optionally_addr
    OWNER to jean_sammet;
	
CREATE TABLE IF NOT EXISTS public.optionally_addr
(
    id text COLLATE pg_catalog."default",
    address_name text COLLATE pg_catalog."default",
	address_type text collate pg_catalog."default"
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;	

CREATE TABLE IF NOT EXISTS public.address_type
(
    id text COLLATE pg_catalog."default",
	address_type text collate pg_catalog."default"
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;	
	
ALTER TABLE IF EXISTS public.address_type
    OWNER to jean_sammet;
	
insert into address_type
values (1,'working_address');

insert into address_type
values (2,'passport_address');

insert into address_type
values (3,'emergency_address');

select * from public.address_type;

----

select * from public.optionally_addr

Insert into public.optionally_addr
values (1,'Street Haki Stermilli',1);

Insert into public.optionally_addr
values (2,'Street Bushi',2);

Insert into public.optionally_addr
values (3,'Street check1',3);

Insert into public.optionally_addr
values (4,'Street check2',1);

Insert into public.optionally_addr
values (5,'Street check3',2);

----
select * from public.users
--"7c4ac61f-b76d-4b45-80bb-0ab13403edfc"
--"9a564d9d-bf64-4ca9-a949-437dc7650b3c"
select * from public.users_optionally_addr

insert into users_optionally_addr
values ('7c4ac61f-b76d-4b45-80bb-0ab13403edfc',1);


insert into users_optionally_addr
values ('7c4ac61f-b76d-4b45-80bb-0ab13403edfc',2);


insert into users_optionally_addr
values ('7c4ac61f-b76d-4b45-80bb-0ab13403edfc',3);


insert into users_optionally_addr
values ('9a564d9d-bf64-4ca9-a949-437dc7650b3c',4);


insert into users_optionally_addr
values ('9a564d9d-bf64-4ca9-a949-437dc7650b3c',5);


--test examples

select t1.first_name, t1.Last_name, t3.address_name, t4.address_type
from users t1 inner join users_optionally_addr t2 on t1.id=t2.user_id
inner join optionally_addr t3 on t2.optianally_address_id=t3.id
inner join address_type t4 on t3.address_type_id=t4.id








#Exercice 4
Create the most optimised index to support the following query: “Find the last 10 transactions with a perfectly round amount (meaning that the cents part is equal to zero, like 35.00 or 120.00) for a given account_id”

select  cast(round( cast(amount as numeric),2) as varchar), account_id, created from public.transactions 
where cast(round( cast(amount as numeric),2) as varchar)  like '%.00' and account_id='46f5d9a5-6292-4b67-a32b-90e44edea5c1'
order by created desc
limit 10;
create INDEX idx_transactions_amount ON transactions (amount);
create index idx_transactions_acc on transactions (account_id);


explain analyze select  cast(round( cast(amount as numeric),2) as varchar), account_id, created from public.transactions 
where cast(round( cast(amount as numeric),2) as varchar)  like '%.00' and account_id='46f5d9a5-6292-4b67-a32b-90e44edea5c1'
order by created desc
limit 10;

create INDEX idx_transactions_amount ON transactions (amount);
create index idx_transactions_acc on transactions (account_id);
