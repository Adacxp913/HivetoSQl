# HivetoSQl
using Hive to query datatable

第一步：本地终端连接操作服务器
第二步：查看对应的表
命令：cd /data/hive
  可以看到文件夹下面有三个对应的表的数据文件：movies.dat【电影表数据】  ratings.dat【评分表数据】  users.dat【用户表数据】
截图：
第三步：进行表数据操作
思路：
1、进入hive目录，创建数据库cxp_movie
2、创建对应的三个表t_user、t_movie、t_rating
3、拷贝本地数据到对应的表
命令：
hive shell

drop database if exists cxp_movie;
create database if not exists cxp_movie;
use cxp_movie;

create table t_user(
userid bigint,
sex string,
age int,
occupation string,
zipcode string) 
row format serde 'org.apache.hadoop.hive.serde2.RegexSerDe' 
with serdeproperties('input.regex'='(.*)::(.*)::(.*)::(.*)::(.*)','output.format.string'='%1$s %2$s %3$s %4$s %5$s')
stored as textfile;

create table t_movie(
movieid bigint,
moviename string,
movietype string) 
row format serde 'org.apache.hadoop.hive.serde2.RegexSerDe' 
with serdeproperties('input.regex'='(.*)::(.*)::(.*)','output.format.string'='%1$s %2$s %3$s')
stored as textfile;

create table t_rating(
userid bigint,
movieid bigint,
rate double,
times string) 
row format serde 'org.apache.hadoop.hive.serde2.RegexSerDe' 
with serdeproperties('input.regex'='(.*)::(.*)::(.*)::(.*)','output.format.string'='%1$s %2$s %3$s %4$s')
stored as textfile;

load data local inpath '/data/hive/movies.dat' into table t_movie;
load data local inpath '/data/hive/ratings.dat' into table t_rating;
load data local inpath '/data/hive/users.dat' into table t_user;

第四步：按照条件进行表数据查询，得到对应的结果
1: 展示电影 ID 为 2116 这部电影各年龄段的平均影评分。
思路：t_user和t_rating表进行联合查询，用moveid=2116作为过滤条件，用年龄段作为分组条件
命令：
select age,avg(rate)as avg_rate
from t_user u join t_rating r
on u.userid=r.userid
where r.movieid=2116
group by u.age;

2: 找出男性评分最高且评分次数超过 50 次的 10 部电影，展示电影名，平均影评分和评分次数。
思路：t_user和t_rating表、t_movie表进行联合查询，用sex="M"作为过滤条件，用m.moviename作为分组条件，选择评分次数超过50次total >=50，按照平均评分倒排avgrate，选取前10条数据
命令：
select "M" as sex,m.moviename as moviename,avg(r.rate) as avgrate,count(m.moviename) as total
from t_rating r
join t_user u
on r.userid=u.userid
join t_movie m
on r.movieid=m.movieid
where u.sex="M"
group by m.moviename
having total >=50
order by avgrate desc
limit 10;

作业3: 找出影评次数最多的女士所给出最高分的 10 部电影的平均影评分，展示电影名和平均影评分（可使用多行 SQL）。
分析思路：
1、需求字段：观影者　t_rating.userid，电影名　t_movie.moviename，影评分　t_rating.rate
2、核心SQL：
(1)需要先求出最喜欢看电影的那位女性:需要查询的字段：性别：t_user.sex，观影次数：count(t_rating.userid)
(2)根据A中求出的女性userid作为where过滤条件，以看过的电影的影评分rate作为排序条件进行排序，求出评分最高的10部电影
需要查询的字段：电影的ID：t_rating.movieid
(3)求出B中10部电影的平均影评分:需要查询的字段：电影的ID：answer5_B.movieid，影评分：t_rating.rate 3、sql：
命令：
(1)需要先求出最喜欢看电影的那位女性
select r.userid,count(r.userid) as total
from t_rating r join t_user u
on r.userid=u.userid
where u.sex="F"
group by r.userid
order by total desc
limit 1;

(2)根据A中求出的女性userid作为where过滤条件，以看过的电影的影评分rate作为排序条件进行排序，求出评分最高的10部电影
create table cxp_answer3 as
select r.movieid as movieid,r.rate as rate
from t_rating r
where r.userid=1150
order by rate desc
limit 10;

select * from cxp_answer3;

(3)求出B中10部电影的平均影评分
create table cxp_answer3_2 as
select r.movieid as movieid,m.moviename as moviename,avg(r.rate) as avgrate
from cxp_answer3 a join t_rating r
on a.movieid=r.movieid
join t_movie m
on r.movieid=m.movieid
group by r.movieid,m.moviename;

select * from cxp_answer3_2;
