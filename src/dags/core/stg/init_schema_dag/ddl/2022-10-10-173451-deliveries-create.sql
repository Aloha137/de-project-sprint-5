create table if not exists stg.deliveries(
id serial unique not null,
object_id  text unique not null,  
object_value text not null,
update_ts timestamp not null
); 