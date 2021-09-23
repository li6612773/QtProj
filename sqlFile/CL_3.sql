insert into cl2_hq_daily
select * from hq_daily_bef_20210101 where trade_date>'2006-01-01'
;
# -- 准备索引
drop index  cl2_hq_daily_trade_date_ts_code_index on cl2_hq_daily ;
create index cl2_hq_daily_trade_date_ts_code_index
	on qtdb.cl2_hq_daily (trade_date, ts_code);

# drop index  cl2_hq_daily_trade_date_high_ts_code_index on cl2_hq_daily ;
# create index cl2_hq_daily_trade_date_high_ts_code_index
# 	on qtdb.cl2_hq_daily (trade_date,high, ts_code);

# drop index  cl2_hq_daily_trade_date_low_ts_code_index on cl2_hq_daily ;
# create index cl2_hq_daily_trade_date_low_ts_code_index
# 	on qtdb.cl2_hq_daily (trade_date,low, ts_code);

drop index hq_trade_cal_exchange_is_open_cal_date_index	on hq_trade_cal ;
create index hq_trade_cal_exchange_is_open_cal_date_index
	on hq_trade_cal (exchange, is_open, cal_date);
-- -------------------------------------
-- 顺序执行一下以下语句，选出下一日股票-使用龙虎榜TopList 数据更全面
-- -------------------------------------
-- 生成龙虎榜里是上涨的股票，并算出吸筹集中度
drop table if exists cl2_cl_jgmx_tmp ;
create table cl2_cl_jgmx_tmp
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
FROM `qtdb`.`hq_TopList` where pct_change >0 and net_rate>25 and   trade_date between '2006-01-01' and '2021-12-31'
;
# select * from cl2_cl_jgmx_tmp order by xgrq desc
;
drop table if exists cl2_cl_jgmx_tmp_xg
;
create table cl2_cl_jgmx_tmp_xg
select a.*, area, industry, market, list_date,datediff(xgrq,list_date) as IPOnDay
from cl2_cl_jgmx_tmp a left join  hq_stock_basic b on a.ts_code = b.ts_code    order by a.xgrq desc
;
# select * from cl2_cl_jgmx_tmp_xg
;
drop table if exists cl2_cl_jgmx
;
create table cl2_cl_jgmx
select * from cl2_cl_jgmx_tmp_xg where IPOnDay>500 and substr(ts_code,1,2) in('00','60')
;
# select * from cl2_cl_jgmx
;
drop table if exists cl2_cl_jg
;
create table cl2_cl_jg
select distinct xgrq,ts_code from cl2_cl_jgmx order by xgrq desc,ts_code desc
;
# select * from cl2_cl_jg order by  xgrq desc
;
# SELECT * FROM qtdb.hq_TopList order by  trade_date desc;
;
# SELECT * FROM qtdb.hq_TopInst order by  trade_date desc;
;
--  ----------------------------------------------回测-------------------------------------------------------
-- 将选出的证券进行回测,选出T+1和T+2的行情
-- 先缓存一张两日关联信息表

drop table if exists cl2_hc_T1
;
drop table if exists cl2_hc_T2
;
drop table if exists cl2_hc_Tn
;
create table cl2_hc_T1
select xgrq,ts_code,get_workdate(xgrq,1) as trade_date_n from cl2_cl_jg
;
create table cl2_hc_T2
select xgrq,ts_code,get_workdate(xgrq,2) as trade_date_n from cl2_cl_jg
;
create table cl2_hc_Tn
select xgrq,ts_code, Case When get_workdate(xgrq,220)>'2021-08-10' Then '2021-08-10'  Else get_workdate(xgrq,220) End  as trade_date_n from cl2_cl_jg
;
select * from cl2_hc_T1 order by xgrq desc
;
select * from cl2_hc_T2 order by xgrq desc
;
select * from cl2_hc_Tn order by xgrq desc
;
-- 取T0、T1、T2行情，有个问题，下一个自然日不一定是工作日，所以会少选一些出来
drop table if exists cl2_hc_jg_tn
;
create table cl2_hc_jg_tn
select a.* ,b.xgrq as trade_date_n,b.xgrq,b.ts_code as xg_ts_code, 'T0' as tn from
 cl2_cl_jg b
left join
 (select * FROM cl2_hq_daily
where trade_date > '2006-01-01') a
on  a.ts_code = b.ts_code and a.trade_date = b.xgrq
;
insert into cl2_hc_jg_tn
select a.* ,b.trade_date_n,b.xgrq,b.ts_code as xg_ts_code, 'T1' as tn from
cl2_hc_T1 b
left join
 (select * FROM cl2_hq_daily
where trade_date > '2006-01-01') a
on  a.ts_code = b.ts_code and a.trade_date = b.trade_date_n
;
insert into cl2_hc_jg_tn
select a.* ,b.trade_date_n,b.xgrq,b.ts_code as xg_ts_code, 'T2' as tn from
cl2_hc_T2 b
left join
 (select * FROM cl2_hq_daily
where trade_date > '2006-01-01') a
on  a.ts_code = b.ts_code and a.trade_date = b.trade_date_n
;
insert into cl2_hc_jg_tn
select a.* ,b.trade_date_n,b.xgrq,b.ts_code as xg_ts_code, 'Tn' as tn from
cl2_hc_Tn b
left join
 (select * FROM cl2_hq_daily
where trade_date > '2006-01-01') a
on  a.ts_code = b.ts_code and a.trade_date = b.trade_date_n
;
select * from cl2_hc_jg_tn order by xgrq desc
;
-- 将新股标识出来，标出上市多少天了 ，除开了新上市的票，提升不明显，降低也不明显，可以踢掉新发的涨停票，避免第二天买不到
drop table if exists cl2_hc_jg_tn_xg
;
create table cl2_hc_jg_tn_xg
select a.*,b.name, area, industry, market, list_date,datediff(xgrq,list_date) as IPOnDay
from cl2_hc_jg_tn a left join  hq_stock_basic b on a.xg_ts_code = b.ts_code    order by a.xgrq desc
;
select * from cl2_hc_jg_tn_xg order by xgrq desc
;
-- 组成包含t1 t2 两天的数据结果
drop table if exists cl2_hc_jg_tn_big01;
drop table if exists cl2_hc_jg_tn_big012;
drop table if exists cl2_hc_jg_tn_big012n;
create table cl2_hc_jg_tn_big01
select t0.*,
    t1.`ts_code` as t1_ts_code,
    t1.`trade_date` as t1_trade_date,
    t1.`open`as t1_open,
    t1.`high`as t1_high,
    t1.`low`as t1_low,
    t1.`close`as t1_close,
    t1.`pre_close`as t1_pre_close,
    t1.`change`as t1_change,
    t1.`pct_chg`as t1_pct_chg,
    t1.`vol`as t1_vol,
    t1.`amount`as t1_amount,
    t1.`xgrq`as t1_xgrq,
    t1.`tn`as t1_tn,
    t1.`name`as t1_name,
    t1.`area`as t1_area,
    t1.`industry`as t1_industry,
    t1.`market`as t1_market,
    t1.`list_date`as t1_list_date,
    t1.`IPOnDay` as t1_IPOnDay
       from (select *from cl2_hc_jg_tn_xg where tn= 'T0') t0 left join (select *from cl2_hc_jg_tn_xg where tn= 'T1') t1 on t0.xgrq = t1.xgrq and t0.xg_ts_code = t1.xg_ts_code
;
select *from cl2_hc_jg_tn_big01;
-- 组成包含t0 t1 t2 两天的数据结果
create table cl2_hc_jg_tn_big012
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
       from (select *from cl2_hc_jg_tn_big01 ) t1 left join (select *from cl2_hc_jg_tn_xg where tn= 'T2') t2 on t1.xgrq = t2.xgrq and t1.ts_code = t2.ts_code
;
create table cl2_hc_jg_tn_big012n
select t1.*,
    tn.`ts_code` as tn_ts_code,
    tn.`trade_date` as tn_trade_date,
    tn.`open`as tn_open,
    tn.`high`as tn_high,
    tn.`low`as tn_low,
    tn.`close`as tn_close,
    tn.`pre_close`as tn_pre_close,
    tn.`change`as tn_change,
    tn.`pct_chg`as tn_pct_chg,
    tn.`vol`as tn_vol,
    tn.`amount`as tn_amount,
    tn.`xgrq`as tn_xgrq,
    tn.`tn`as tn_tn,
    tn.`name`as tn_name,
    tn.`area`as tn_area,
    tn.`industry`as tn_industry,
    tn.`market`as tn_market,
    tn.`list_date`as tn_list_date,
    tn.`IPOnDay` as tn_IPOnDay
       from (select *from cl2_hc_jg_tn_big012 ) t1 left join (select *from cl2_hc_jg_tn_xg where tn= 'tn') tn on t1.xgrq = tn.xgrq and t1.ts_code = tn.ts_code
;

select * from cl2_hc_jg_tn_big012n order by xgrq desc ;
-- 在选股第二天，能够成功以昨收价买入的，且以收盘价卖出后的利润率 （除去一年内的新股，只选深圳主板的票）
drop table if exists cl2_hc_jg_tn_lrl_4kind ;
create table cl2_hc_jg_tn_lrl_4kind
select *,(Case When t1_low<=t1_open Then t2_close - t1_open Else 0 End)*100/t1_open  as lrl_t1openjMR_t2closejMC,
       (Case When t1_low<=close Then t2_close - close Else 0 End)*100/close  as lrl_t0closejMR_t2closejMC,
       (Case When t1_low<=t1_open Then tn_close - t1_open Else 0 End)*100/t1_open  as lrl_t1openjMR_tnclosejMC,
       (Case When t1_low<=close Then tn_close - close Else 0 End)*100/close  as lrl_t0closejMR_tnclosejMC
from cl2_hc_jg_tn_big012n   where  name not like '%ST%' order by xgrq desc ;

select COUNT(*) from cl2_hc_jg_tn_lrl_4kind where lrl_t1openjMR_tnclosejMC=0 ;
select COUNT(*) from cl2_hc_jg_tn_lrl_4kind where lrl_t1openjMR_tnclosejMC<>0 ;

select sum(lrl_t1openjMR_tnclosejMC) as lrl_t1openjMR_tnclosejMC,sum(lrl_t1openjMR_tnclosejMC)/(select count(*) from cl2_hc_jg_tn_lrl_4kind where lrl_t1openjMR_tnclosejMC<>0) as 平均每只收益率 ,
       sum(lrl_t0closejMR_tnclosejMC) as lrl_t0closejMR_tnclosejMC  from cl2_hc_jg_tn_lrl_4kind ;

select sum(lrl_t1openjMR_tnclosejMC) as lrl_t1openjMR_tnclosejMC,sum(lrl_t1openjMR_tnclosejMC)/(select count(*) from cl2_hc_jg_tn_lrl_4kind where lrl_t1openjMR_tnclosejMC<>0) as 平均每只收益率 ,
       sum(lrl_t0closejMR_tnclosejMC) as lrl_t0closejMR_tnclosejMC  from cl2_hc_jg_tn_lrl_4kind where tn_trade_date <>'2021-08-10';

#  算出持股期间（xgrq --- curentdate）的最低价（每日行情的最低价）和最高价（每日行情的最高价）
drop table if exists cl2_hc_jg_tn_lrl_4kind_hl
;
create table cl2_hc_jg_tn_lrl_4kind_hl
select *,(select max(high) from cl2_hq_daily where ts_code=a.ts_code and trade_date between get_workdate(a.xgrq,2) and tn_trade_date) as highest_price,
       (select min(low) from cl2_hq_daily where ts_code=a.ts_code and trade_date between get_workdate(a.xgrq,2) and tn_trade_date) as lowest_price from cl2_hc_jg_tn_lrl_4kind a
;
select * from cl2_hc_jg_tn_lrl_4kind_hl ;
select  (Case When(highest_price-t1_open)*100/t1_open>=40 Then 40 Else lrl_t1openjMR_tnclosejMC End)  as 止盈后收益率,
       (Case When(highest_price-t1_open)*100/t1_open>=40 Then t1_open*0.4 Else tn_close-t1_open End) as 止盈后收益（元）,
       ts_code as 股票代码,name as 股票简称,xgrq as 选股日期,t1_trade_date as 买入日期 ,t1_open as 买入价,
       tn_trade_date as 持有到期日期,tn_close as 到期卖出价,tn_close-t1_open as 持有到期盈利（元）,
       highest_price as 历史最高价,lowest_price as 历史最低价 ,
       (highest_price-t1_open)*100/t1_open  as 最高价涨幅, (lowest_price-t1_open)*100/t1_open as 最低价跌幅 ,a.*
from cl2_hc_jg_tn_lrl_4kind_hl a ;
-- 把最高价和最低价出现的日期加进去
drop table if exists cl2_hc_jg_tn_lrl_4kind_highlowWithdate
;
create table cl2_hc_jg_tn_lrl_4kind_highlowWithdate
select *,(select max(trade_date) from cl2_hq_daily where ts_code=a.ts_code and high=a.highest_price and trade_date between get_workdate(a.xgrq,2) and tn_trade_date) as highest_price_date,
       (select max(trade_date) from cl2_hq_daily where ts_code=a.ts_code and low=a.lowest_price and trade_date between get_workdate(a.xgrq,2) and tn_trade_date) as lowest_price_date from cl2_hc_jg_tn_lrl_4kind_hl a
;
select  * from cl2_hc_jg_tn_lrl_4kind_highlowWithdate
;
drop table if exists cl2_hc_jg_tn_lrl_4kind_hl_tmp
;
create table cl2_hc_jg_tn_lrl_4kind_hl_tmp
select ts_code as 股票代码,name as 股票简称,xgrq as 选股日期,t1_trade_date as 买入日期 ,t1_open as 买入价,tn_trade_date as 持有到期日期,tn_close as 到期卖出价,tn_close-t1_open as 持有到期盈利（元）,
       highest_price as 历史最高价,highest_price_date as 历史最高价出现日期,lowest_price as 历史最低价,lowest_price_date as 历史最低价出现日期 ,
       (highest_price-t1_open)*100/t1_open  as 最高价涨幅, (lowest_price-t1_open)*100/t1_open as 最低价跌幅 ,a.*
from cl2_hc_jg_tn_lrl_4kind_highlowWithdate a
;
select * from cl2_hc_jg_tn_lrl_4kind_hl_tmp ;
--  止损-n后的收益
select sum(lrl_t1openjMR_tnclosejMC) +  (select count(*)*-20 from cl2_hc_jg_tn_lrl_4kind_hl_tmp where 最低价跌幅 <-20) as fh from cl2_hc_jg_tn_lrl_4kind_hl_tmp where 最低价跌幅 >=-20
;
--  止赢n后的收益(明细）
select (Case When 最高价涨幅>=40 Then 40 Else lrl_t1openjMR_tnclosejMC End)  as 止盈后收益率,(Case When 最高价涨幅>=40 Then t1_open*0.4 Else tn_close-t1_open End) as 止盈后收益（元）,a.* from cl2_hc_jg_tn_lrl_4kind_hl_tmp a
;
-- 把止盈的日期加进去
drop table if exists cl2_hc_jg_tn_lrl_4kind_hl_jg;
create table cl2_hc_jg_tn_lrl_4kind_hl_jg
select (Case When 最高价涨幅>=40 Then 40 Else lrl_t1openjMR_tnclosejMC End)  as 止盈后收益率,(Case When 最高价涨幅>=40 Then t1_open*0.4 Else tn_close-t1_open End) as 止盈后收益（元）,
       t1_open*1.4 as 止盈卖出价,
       (select min(trade_date) from cl2_hq_daily where ts_code=a.ts_code and high>=t1_open*1.4 and trade_date between get_workdate(a.xgrq,2) and tn_trade_date ) as 止盈卖出日期 ,
       a.* from cl2_hc_jg_tn_lrl_4kind_hl_tmp a
;
-- 加入龙虎榜信息
select * from cl2_hc_jg_tn_lrl_4kind_hl_jg ;
select b.net_rate as 净买入占比,b.pct_change as 选股当日涨幅,a.*,b.* from cl2_hc_jg_tn_lrl_4kind_hl_jg a left join cl2_cl_jgmx b on a.xgrq= b.xgrq and a.ts_code= b.ts_code ;

-- 止赢n后的收益（总收益，平均每只收益）
select sum(Case When 最高价涨幅>=40 Then 40 Else lrl_t1openjMR_tnclosejMC End) as 总收益,
       sum(Case When 最高价涨幅>=40 Then 40 Else lrl_t1openjMR_tnclosejMC End)/(select count(*) from cl2_hc_jg_tn_lrl_4kind where lrl_t1openjMR_tnclosejMC<>0) as 平均每只收益率
       from cl2_hc_jg_tn_lrl_4kind_hl_jg ;




select lrl_t1openjMR_tnclosejMC,最高价涨幅,最低价跌幅,(Case When 最高价涨幅>=40 Then 40 Else lrl_t1openjMR_tnclosejMC End) as 止盈后收益,a.*
from cl2_hc_jg_tn_lrl_4kind_hl_tmp a ;

select Case When t1_low<=close Then t2_close - close Else 0 End from cl2_hc_jg_tn_lrl_4kind_hl_tmp where lrl_t1openjMR_tnclosejMC<最高价涨幅
;




