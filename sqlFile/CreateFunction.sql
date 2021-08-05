CREATE DEFINER=`root`@`%` FUNCTION `get_workdate`(pcdate date, pn int) RETURNS date
BEGIN
declare ndate date;
    select max(a.cal_date) as cal_date from (SELECT cal_date FROM qtdb.hq_trade_cal where exchange = 'SSE' and is_open = '1'
    and cal_date > pcdate order by cal_date limit pn) a into ndate;
RETURN ndate;
END