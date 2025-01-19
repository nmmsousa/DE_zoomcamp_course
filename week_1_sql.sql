ALTER TABLE public.green_taxi_data
ALTER COLUMN lpep_pickup_datetime TYPE TIMESTAMP USING lpep_pickup_datetime::TIMESTAMP,
ALTER COLUMN lpep_dropoff_datetime TYPE TIMESTAMP USING lpep_dropoff_datetime::TIMESTAMP;

select case when trip_distance <=1 then 'Up to 1 mile'
when trip_distance >1 and trip_distance <=3 then 'In between 1 (exclusive) and 3 miles (inclusive)'
when trip_distance >3 and trip_distance <=7 then 'In between 3 (exclusive) and 7 miles (inclusive)'
when trip_distance >7 and trip_distance <=10 then 'In between 7 (exclusive) and 10 miles (inclusive)'
when trip_distance >10 then 'Over 10 miles'
else 'others'
end as trip_count,
count (*)
from public.green_taxi_data g
where g.lpep_pickup_datetime::date >= '2019-01-01'
and g.lpep_dropoff_datetime::date <= '2019-10-31'
group by trip_count;


select g.lpep_pickup_datetime::date, max(g.trip_distance) as top_distance 
from public.green_taxi_data g
group by g.lpep_pickup_datetime::date
order by  max(g.trip_distance) desc


select lz."Zone", sum(g.total_amount) as pu_total_amount from public.green_taxi_data g
inner join public.lookup_zones lz on lz."LocationID" = g."PULocationID"
where g.lpep_pickup_datetime::date = '2019-10-18'
group by lz."Zone"
having sum(g.total_amount) > 13000;

with cte as (
select *
from public.green_taxi_data g
inner join public.lookup_zones lz on lz."LocationID" = g."DOLocationID"
where g.lpep_pickup_datetime::date >= '2019-01-01'
and g.lpep_dropoff_datetime::date <= '2019-10-31'
and g."PULocationID" = 74)
select cte."Zone", max(tip_amount) from cte
group by cte."Zone"
order by max(tip_amount) desc;