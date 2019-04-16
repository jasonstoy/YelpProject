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

/*===============================*/
drop table if exists analytics.top_review_num_toronto;
create table analytics.top_review_num_toronto as (
  SELECT name,business_id,review_count,address,stars,latitude,longitude
  from history.business1
  where city like 'Toronto'
  order by review_count desc
  limit 100
);
/*================================*/
drop table if exists analytics.top_rating_num_toronto;
create table analytics.top_rating_num_toronto as (
  SELECT name,business_id,review_count,address,stars,latitude,longitude
  from history.business1
  where city like 'Toronto'
  order by review_count desc, stars desc
  limit 100
);
/*===================================*/
drop table if exists flow.review_flow;
create table flow.review_flow(
  name varchar,
  userid varchar,
  username varchar,
  userprofile varchar,
  rating int,
  created timestamp,
  review varchar,
  image varchar
);
