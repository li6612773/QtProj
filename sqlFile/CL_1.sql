-- -------------------------------------
-- 顺序执行一下以下语句，选出下一日股票  使用TopInst 数据更准确
-- -------------------------------------
-- 生成龙虎榜里是上涨的股票，并算出吸筹集中度
drop table cl1_cl_LHB_Rise
;
create table cl1_cl_LHB_Rise SELECT trade_date,ts_code,sum(buy_rate) as buy_rate_sum,sum(sell_rate) as sell_rate_sum
 FROM qtdb.hq_TopInst  where  trade_date>'2021-01-01' and reason like '%涨幅%'  group by trade_date,ts_code
;
select * from cl1_cl_LHB_Rise order by trade_date desc
;
-- 选出吸筹集中度高的票
drop table cl1_cl_LHB_Rise_AND_GJZD
;
create table cl1_cl_LHB_Rise_AND_GJZD
select trade_date as xgrq,ts_code,buy_rate_sum,sell_rate_sum,(buy_rate_sum - sell_rate_sum ) as b_s
           From cl1_cl_LHB_Rise b
;
select * from cl1_cl_LHB_Rise_AND_GJZD order by xgrq desc,ts_code desc
;
drop table cl1_cl_jg
;
create table cl1_cl_jg
select * from cl1_cl_LHB_Rise_AND_GJZD where buy_rate_sum - sell_rate_sum > 10 order by xgrq desc,ts_code desc
;
select * from cl1_cl_jg order by  xgrq desc
;
SELECT * FROM qtdb.hq_TopList order by  trade_date desc;
;
SELECT * FROM qtdb.hq_TopInst order by  trade_date desc;
;
--  ----------------------------------------------回测-------------------------------------------------------
-- 将选出的证券进行回测,选出T+1和T+2的行情
-- 先缓存一张两日关联信息表
drop table cl1_hc_T1
;
drop table cl1_hc_T2
;
create table cl1_hc_T1
select concat(ts_code,date_sub(xgrq,interval -1 day)) as glxx,xgrq from cl1_cl_jg
;
create table cl1_hc_T2
select concat(ts_code,date_sub(xgrq,interval -2 day)) as glxx,xgrq from cl1_cl_jg
;
select * from cl1_hc_T1 order by xgrq desc
;
select * from cl1_hc_T2 order by xgrq desc
;
drop table cl1_hc_jg
;
-- 取T+1行情，有个问题，下一个自然日不一定是工作日，所以会少选一些出来
create table cl1_hc_jg
SELECT a.*,date_sub(trade_date,interval 1 day) as xgrq,'T1' as tn
    FROM hq_daily a
    where a.trade_date > '2021-01-01'
      and concat(a.ts_code, a.trade_date) in (select glxx from cl1_hc_T1 )
;
insert into cl1_hc_jg
SELECT a.*,date_sub(trade_date,interval 2 day) as xgrq,'T2' as tn
    FROM hq_daily a
    where a.trade_date > '2021-01-01'
      and concat(a.ts_code, a.trade_date) in (select glxx from cl1_hc_T2 )
;
select * from cl1_hc_jg order by xgrq desc
;
-- 将新股标识出来，标出上市多少天了 ，除开了新上市的票，提升不明显，降低也不明显，可以踢掉新发的涨停票，避免第二天买不到
drop table cl1_hc_jg1
;
create table cl1_hc_jg1
select a.*,b.name, area, industry, market, list_date,datediff(current_date(),list_date) as IPOnDay
from cl1_hc_jg a left join  hq_stock_basic b on a.ts_code = b.ts_code    order by a.xgrq desc
;
select * from cl1_hc_jg1 order by xgrq desc





