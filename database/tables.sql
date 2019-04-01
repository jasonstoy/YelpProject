create table history.master as(

select * from history.master1
union all
select * from history.master2
union all
select * from history.master3
union all
select * from history.master4
union all
select * from history.master5
union all
select * from history.master6
union all
select * from history.master7
);

/*==============================*/
create table analytics.top_master1 as (
  SELECT business_id,sum(stars) as total_stars
  FROM history.master1
  group by business_id
  order by sum(stars) desc
  limit 20
);
