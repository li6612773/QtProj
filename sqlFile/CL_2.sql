-- -------------------------------------
-- 顺序执行一下以下语句，选出下一日股票-使用龙虎榜TopList 数据更全面
-- -------------------------------------
-- 生成龙虎榜里是上涨的股票，并算出吸筹集中度
drop table cl2_cl_jgmx ;
create table cl2_cl_jgmx
SELECT trade_date as xgrq,ts_code,
	`hq_TopList`.`net_rate`,
    `hq_TopList`.`amount_rate`,
    `hq_TopList`.`name`,
    `hq_TopList`.`close`,
    `hq_TopList`.`pct_change`,
    `hq_TopList`.`turnover_rate`,
    `hq_TopList`.`amount`,
    `hq_TopList`.`l_sell`,
    `hq_TopList`.`l_buy`,
    `hq_TopList`.`l_amount`,
    `hq_TopList`.`net_amount`,

    `hq_TopList`.`float_values`,
    `hq_TopList`.`reason`
FROM `qtdb`.`hq_TopList` where pct_change >0 and net_rate>10 and   trade_date>'2021-01-01'
;
select * from cl2_cl_jgmx order by xgrq desc
;
drop table cl2_cl_jg
;
create table cl2_cl_jg
select distinct xgrq,ts_code from cl2_cl_jgmx order by xgrq desc,ts_code desc
;


select * from cl2_cl_jg order by  xgrq desc
;
SELECT * FROM qtdb.hq_TopList order by  trade_date desc;
;
SELECT * FROM qtdb.hq_TopInst order by  trade_date desc;
;
--  ----------------------------------------------回测-------------------------------------------------------
-- 将选出的证券进行回测,选出T+1和T+2的行情
-- 先缓存一张两日关联信息表
drop table cl2_hc_T0
;
drop table cl2_hc_T1
;
drop table cl2_hc_T2
;
create table cl2_hc_T0
select concat(ts_code,xgrq) as glxx,xgrq from cl2_cl_jg
;
create table cl2_hc_T1
select concat(ts_code,date_sub(xgrq,interval -1 day)) as glxx,xgrq from cl2_cl_jg
;
create table cl2_hc_T2
select concat(ts_code,date_sub(xgrq,interval -2 day)) as glxx,xgrq from cl2_cl_jg
;
select * from cl2_hc_T0 order by xgrq desc
;
select * from cl2_hc_T1 order by xgrq desc
;
select * from cl2_hc_T2 order by xgrq desc
;
drop table cl2_hc_jg_tn
;
-- 取T0、T1、T2行情，有个问题，下一个自然日不一定是工作日，所以会少选一些出来
create table cl2_hc_jg_tn
SELECT a.*,trade_date as xgrq,'T0' as tn
    FROM hq_daily a
    where a.trade_date > '2021-01-01'
      and concat(a.ts_code, a.trade_date) in (select glxx from cl2_hc_T0 )
;
insert into cl2_hc_jg_tn
SELECT a.*,date_sub(trade_date,interval 1 day) as xgrq,'T1' as tn
    FROM hq_daily a
    where a.trade_date > '2021-01-01'
      and concat(a.ts_code, a.trade_date) in (select glxx from cl2_hc_T1 )
;
insert into cl2_hc_jg_tn
SELECT a.*,date_sub(trade_date,interval 2 day) as xgrq,'T2' as tn
    FROM hq_daily a
    where a.trade_date > '2021-01-01'
      and concat(a.ts_code, a.trade_date) in (select glxx from cl2_hc_T2 )
;
select * from cl2_hc_jg_tn order by xgrq desc
;
-- 将新股标识出来，标出上市多少天了 ，除开了新上市的票，提升不明显，降低也不明显，可以踢掉新发的涨停票，避免第二天买不到
drop table cl2_hc_jg_tn_t1
;
create table cl2_hc_jg_tn_t1
select a.*,b.name, area, industry, market, list_date,datediff(current_date(),list_date) as IPOnDay
from cl2_hc_jg_tn a left join  hq_stock_basic b on a.ts_code = b.ts_code    order by a.xgrq desc
;
select * from cl2_hc_jg_tn_t1 order by xgrq desc

-- 组成包含t1 t2 两天的数据结果
drop table cl2_hc_jg_tn_big;
create table cl2_hc_jg_tn_big
select t1.*,
    t2.`ts_code` as t2_ts_code,
    t2.`trade_date` as t2_trade_date,
    t2.`open`as t2_open,
    t2.`high`as t2_high,
    t2.`low`as t2_low,
    t2.`close`as t2_close,
    t2.`pre_close`as t2_pre_close,
    t2.`change`as t2_change,
    t2.`pct_chg`as t2_pct_chg,
    t2.`vol`as t2_vol,
    t2.`amount`as t2_amount,
    t2.`xgrq`as t2_xgrq,
    t2.`tn`as t2_tn,
    t2.`name`as t2_name,
    t2.`area`as t2_area,
    t2.`industry`as t2_industry,
    t2.`market`as t2_market,
    t2.`list_date`as t2_list_date,
    t2.`IPOnDay` as t2_IPOnDay
       from (select *from cl2_hc_jg_tn_t1 where tn= 'T1') t1 left join (select *from cl2_hc_jg_tn_t1 where tn= 'T2') t2 on t1.xgrq = t2.xgrq and t1.ts_code = t2.ts_code
;
-- 在选股第二天，能够成功以昨收价买入的，且以收盘价卖出后的利润率 （除去一年内的新股，只选深圳主板的票）
drop table cl2_hc_jg_tn_lrl ;
create table cl2_hc_jg_tn_lrl
select *,(close-pre_close)*100/pre_close as lrl,(cl2_hc_jg_tn_big.t2_close-pre_close)*100/pre_close as lrl2  from cl2_hc_jg_tn_big where tn= 'T1'and IPOnDay>300 and pre_close >low  and substr(ts_code,1,2) in('00','60')
and name not like '%ST%' order by xgrq desc ;

select * from cl2_hc_jg_tn_lrl;




