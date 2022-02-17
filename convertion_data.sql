with -- 团 
groupon_detail as (
    -- 团购详情
    select distinct
        gu.user_id
        ,date_format(gu.created_time, '%Y-%m') as mon
        ,gu.created_time
        ,case gu.identity
            when 0 then '团长'
            when 1 then '团员'
            else 'others' end as identity
        ,gu.group_id
        ,gu.sku_code
        ,sku.spu_name
        ,case gu.group_status
            when 0 then -1 -- 拼团中，历史数据应无此status
            when 1 then 1 -- 拼团成功
            when 2 then 0 -- 拼团失败
        end as group_success
        ,od.channel_id chi
    from bdp_dw.dw_s_suyang_user_group_buy_s gu -- 团购表
    left join bdp_dw.dw_o_center_order_s od on gu.order_id = od.id -- for取订单信息
    left join bdp_dim.dim_channel_info_s c on od.channel_id = c.channel_id
    left join bdp_dim.dim_mall_product_s sku on gu.sku_code = sku.sku_code
    where od.status not in (0, 850)
        -- and c.channel_third in ('主课时拼团') 
        and sku.spu_name like '%15节思维训练课%'
        and od.pay_time is not null -- 不计支付取消的
)
,group_info as (
    -- 主课时拼团团信息，整合团长团员uid
    select
        f.*
        ,m.member_id
    from (
        select group_id
            ,mon
            ,sku_code
            ,user_id founder_id
            ,group_success
            ,chi
            ,row_number() over (partition by user_id,identity order by created_time asc) found_rnn -- 团长第几次开团
        from groupon_detail
        where identity = '团长'
    ) f
    left join (
        select group_id
            , sku_code
            ,array_agg(user_id) member_id
        from groupon_detail
        where identity = '团员'
        group by 1,2
    ) m on f.group_id = m.group_id
    group by 1,2,3,4,5,6,7,8
)
 -- 团长相关
,paid_user as
(-- 用户生命周期
    select  a.user_id
        ,case when pay_mon = datedate then 'M0'
             when date_diff('month',date(pay_mon),date(datedate)) in (1,2) then 'M1-M2'
             when date_diff('month',date(pay_mon),date(datedate)) in (3,4,5,6) then 'M3-M6'  
        else 'M7+' end lifetime
        ,date_diff('month',date(pay_mon),date(datedate)) as lifecycle
        ,substr(b.datedate,1,7) mon
        ,pay_date
    from
    (
        select  user_id
               ,date_format(pay_time,'%Y-%m-01') pay_mon
               ,date_format(pay_time,'%Y-%m-%d') pay_date
        from
        (
            select  o.user_id
                   ,o.pay_time
                   ,o.package_order_id
                   ,row_number() over (partition by o.user_id order by o.pay_time) rnn
            from bdp_dmd.dmd_order_s o
            where o.course_package_type = '1' -- 系统/正式课购买（与试听课对立）
            and o.is_short_course = 0 -- 非短期课
            and o.course_package_teaching_method != 1 -- 非AI课（与0直播课对立）
            and o.pay_time is not null -- 识别订单是否有效
            and o.is_mistake_order = 0 -- 排除错误单
            and o.is_test_order = 0 -- 非测试单
            and o.order_type <> 90 -- 剔除积分兑换（积分兑换不算购买订单 新签一般没有）
            and o.user_id not in (8, 602040) -- 非测试账户 谢楠&李赫明
            -- and o.refund_time is null -- 剔除掉退费用户
            and o.subject_type in (0, 1) 
        )
        where rnn = 1 
    ) a , (
    select  concat(substr(datedate,1,7),'-01') datedate
    from bdp_dim.dim_calendar_s c
    where datedate between '2021-07' and '2022-02'
    group by  1 ) b
    where datedate >= pay_mon
)
,ref_tmp as (
    -- leads转介绍关系临时表
    select leads_from_user_id
        ,mon
        ,leads_id
        ,chi
        ,touch_mon
        ,order_mon
    from (
        select s.leads_from_user_id
            ,if(c.channel_third = '主课时拼团',1) chi
            ,s.leads_id
            ,date_format(h.is_cc_touch_time,'%Y-%m') touch_mon
            ,date_format(h.first_order_time,'%Y-%m') order_mon
        from bdp_dmd.dmd_leads_s s
        left join bdp_dmse.dmse_leads_h h on s.leads_id = h.leads_id
        left join bdp_dim.dim_channel_info_s c on s.init_channel_id = c.channel_id
        where date(h.data_dt) = current_date
            and s.is_overseas = 0
            and s.subject = 1 -- 数学国内
            and s.leads_from_user_id is not null
    ), (
        select substr(datedate,1,7) mon
        from bdp_dim.dim_calendar_s
        where substr(datedate,1,7) between '2021-07' and '2022-02'
        group by 1
    )
    group by 1,2,3,4,5,6
)

,founder_referral as (
    -- 团长带R能力
    select t.user_id
        , mon
        , count(distinct if(touch_mon <= mon, leads_id)) referral_leads
        , count(distinct if(order_mon <= mon, leads_id)) referral_orders
        , count(distinct if(touch_mon <= mon and chi = 1, leads_id)) referral_group_leads
        , count(distinct if(order_mon <= mon and chi = 1, leads_id)) referral_group_orders
    from (
            select user_id 
            from groupon_detail
            where identity = '团长'
            group by 1
            ) t 
    join ref_tmp on t.user_id = ref_tmp.leads_from_user_id
    group by 1,2
)

,founder_in_class as (
    select  -- 是否休眠，未休眠就是在读 
        substr(data_dt,1,7) mon
        ,user_id
        ,case max(grade)
            when 0 then 0
            when 1 then 1
            when 2 then 2
            when 3 then 3
            when 4 then 4
            when 5 then 5
            when 6 then 6
            when 7 then 1
            when 8 then 7
            when 9 then 8
            when 10 then 8
            else  -999
        end as  level
    from bdp_dw.dw_k_service_plan_h
    where data_dt in (
            select datedate
            from(
                select substr(datedate,1,7) mon
                    ,min(datedate) datedate
                from bdp_dim.dim_calendar_s
                where substr(datedate,1,7) between '2021-07' and '2022-02'
                group by 1
                )
            group by 1
            )   
        and deleted = 0
        and la_id <> 0 
        and status = 1
        and subject in (0,1)
    group by 1,2

)

-- 火花币
, spark_coin_detail as (
    -- by 月
    select  user_id
        ,date(date_format(a.created_time, '%Y-%m-01')) mon                                            
        ,sum(if(op_type in (1,3,6,9),a.op_amount,null)) issued
        ,sum(if(op_type = 2,a.op_amount,null)) exchanged
    from bdp_dw.dw_a_mass_asset_op_log_s a
    where op_type in (1, 2, 3, 6, 9)
        and date_format(a.created_time, '%Y-%m')>='2020-07' -- 过期时间：一年
    group by  1,2
    )

, spark_coin as (
    select t.user_id
        , date_format(t.mon,'%Y-%m') mon
        , avg(if(date_diff('month',sc.mon,t.mon) between 0 and 12,issued)) issued
        , avg(if(date_diff('month',sc.mon,t.mon) between 0 and 12,exchanged)) exchanged
    from(
        select user_id
            ,date(mon) mon
        from (
            select user_id 
            from groupon_detail
            where identity = '团长'
            group by 1
            ) , (
                select substr(datedate,1,7) col1
                    ,min(datedate) mon
                from bdp_dim.dim_calendar_s
                where substr(datedate,1,7) between '2021-07' and '2022-02'
                group by 1
            )
         group by 1,2
        ) t
    join spark_coin_detail sc on t.user_id = sc.user_id
    group by 1,2  
)

, user_info as
(-- 用户城市等级、注册时间（海外？）
    select  user_id
        ,overseas_customers
        ,city_level_new city_level
        ,province_name
        ,phone_city_name
        ,a.regist_time 
        ,a.channel_id
    from bdp_dmd.dmd_user_s a
    left join bdp_dim.dim_city_s b on a.phone_city_name = b.city_name_new
    group by  1,2,3,4,5,6,7
)

-- Leads相关
,trail_info as 
(-- Leads试听课
    select 
        leads_id 
        ,max(if(rnn1=1, levels)) fst_apt_level
        ,max(if(rnn2=1, levels)) lst_apt_level
        ,max(rnn1) apt_times
        ,count(distinct valid_finish_time) fin_times
        ,count(distinct attend_time) atd_times
    from (
        select 
            leads_id
            , levels
            , valid_finish_time
            , attend_time
            -- , duration -- 试听课出席时长 2019-11-28开始有数据
            , row_number() over (partition by user_id order by created_time asc) as rnn1
            , row_number() over (partition by user_id order by created_time desc) as rnn2
        from bdp_mid_as.dm_classroom_student_h
        where date(data_dt) = current_date
            and course_type = 4 -- 试听课
            and student_classroom_status = 1 -- 1正常，0停用
            and student_appointed_time is not null
    )
    -- where rnn1 * rnn2 = 1
    group by 1
)
,leads_duration as (
    select s.leads_id
        ,s.user_id
        ,s.leads_from_user_id
        ,h.is_cc_touch_time
        ,s.init_channel_id
        ,case when regexp_like(upper(c.channel_five_value),'CC|H5') then '直链'
            when regexp_like(upper(c.channel_five_value),'PUSH') then 'push'
            else '家长端'
        end as channel_fifth
        ,date_format(h.is_cc_touch_time,'%Y-%m') mon
        ,date_diff('hour',h.is_cc_touch_time,h.first_connected_time) fst_net_diff
        ,date_diff('hour',h.is_cc_touch_time,h.first_appointed_time) fst_apt_diff
        ,date_diff('hour',h.is_cc_touch_time,h.first_attend_time) fst_atd_diff
        ,date_diff('hour',h.is_cc_touch_time,h.first_valid_finish_time) fst_fin_diff
        ,date_diff('hour',h.is_cc_touch_time,h.first_order_time) fst_ord_diff
    from bdp_dmd.dmd_leads_s s 
    join bdp_dim.dim_channel_info_s c on s.init_channel_id = c.channel_id
    left join bdp_dmse.dmse_leads_h h on s.leads_id = h.leads_id
    where 1 = 1
        and date(h.data_dt) = current_date
        and date_format(h.is_cc_touch_time,'%Y-%m-%d') between '2021-07' and '2022-02'
        and s.is_overseas = 0 
        and s.subject = 1
        and c.channel_third = '主课时拼团'
    group by 1,2,3,4,5,6,7,8,9,10,11,12
)

---- summary
select a.leads_id
    ,a.mon
    ,a.is_cc_touch_time
    ,a.user_id
    ,a.leads_from_user_id
    -- 团
    ,case when a.leads_from_user_id = g.founder_id then 1
        when g.group_id is not null then 0
    end as invited_by
    ,g.found_rnn founder_grouping_times
    ,g.group_success
    -- 团长
    ,if(uf.overseas_customers = 1,99,uf.city_level) founder_city_level
    ,if(uf.overseas_customers = 1,'海外',uf.province_name) founder_province_name
    ,fc.level founder_cls_max_level
    ,pu.lifetime founder_lifetime
    ,pu.lifecycle founder_lifetime_detail
    ,fr.referral_leads founrder_ref_leads
    ,fr.referral_orders founrder_ref_ords
    ,fr.referral_group_leads founrder_ref_group_leads
    ,fr.referral_group_orders founrder_ref_group_ords
    ,sc.issued founder_avg_issued_coin
    ,sc.exchanged founder_avg_exchanged_coin
    -- Leads
    ,if(ul.overseas_customers = 1,99,ul.city_level) leads_city_level
    ,if(ul.overseas_customers = 1,'海外',ul.province_name) leads_province_name
    ,a.channel_fifth
    ,if(a.init_channel_id = ul.channel_id,1,0) leads_chi_eql_reg
    -- leads间隔
    ,date_diff('hour',a.is_cc_touch_time,ul.regist_time) fst_reg_diff
    ,a.fst_net_diff
    ,fst_apt_diff
    ,fst_atd_diff
    ,fst_fin_diff
    -- Leads试听课
    ,tr.apt_times
    ,tr.fst_apt_level
    ,tr.lst_apt_level
    ,tr.fin_times
    ,tr.atd_times
    -- 转化
    ,fst_ord_diff
from leads_duration a
left join group_info g on a.mon = g.mon and contains(g.member_id,a.user_id)
left join user_info uf on g.founder_id = uf.user_id
left join founder_in_class fc on g.founder_id = fc.user_id and a.mon = fc.mon
left join paid_user pu on g.founder_id = pu.user_id and a.mon = pu.mon
left join founder_referral fr on g.founder_id = fr.user_id and a.mon = fr.mon
left join spark_coin sc on g.founder_id = sc.user_id and a.mon = sc.mon
left join user_info ul on a.user_id = ul.user_id
left join trail_info tr on a.leads_id  = tr.leads_id
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
    ,23,24,25,26,27,28,29,30,31,32,33,34
