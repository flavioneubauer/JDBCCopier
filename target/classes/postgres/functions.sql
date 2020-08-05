create or replace function disable_trigger_func(a boolean) returns void as
$$
declare 
act character varying;
r record;
begin
    if(a is true) then
        act = 'disable';
    else
        act = 'enable';
    end if;

    for r in select * from information_schema.tables where table_schema = 'public' 
    loop
        execute format('alter table %I %s trigger all', r.table_name, act); 
    end loop;
end;
$$
language plpgsql;

CREATE or replace FUNCTION boolean1(i smallint) RETURNS boolean AS $$
    BEGIN
            RETURN (i::smallint)::int::bool;
    END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION boolean2(i numeric) RETURNS boolean AS $$
    BEGIN
            RETURN (i::numeric)::int::bool;
    END;
$$ LANGUAGE plpgsql;

drop cast if exists (smallint as boolean);
drop cast if exists (numeric as boolean);

CREATE CAST (smallint AS boolean) WITH FUNCTION boolean1(smallint) AS ASSIGNMENT;
CREATE CAST (numeric AS boolean) WITH FUNCTION boolean2(numeric) AS ASSIGNMENT;