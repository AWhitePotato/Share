with paid_user as
(-- 用户生命周期
    select  a.user_id
           ,case when pay_mon = datedate then 'M0'
                 when date_diff('month',date(pay_mon),date(datedate)) in (1,2) then 'M1-M2'
                 when date_diff('month',date(pay_mon),date(datedate)) in (3,4,5,6) then 'M3-M6'  
            else 'M7+' end lifecycle
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
            and o.refund_time is null -- 剔除掉退费用户
            and o.subject_type in (0, 1) 
        )
        where rnn = 1 
    ) a , (
    select  concat(substr(datedate,1,7),'-01') datedate
    from bdp_dim.dim_calendar_s c
    where datedate between concat({{start_mon}},'-01') and concat({{end_mon}},'-01')
    group by  1 ) b
    where datedate >= pay_mon
)
, mau_paid_user as (
    select
        paid_user.mon
        , lifecycle
        , if(pay_date <= data_dt, track.uid) as user_id
    from bdp_dw.dw_u_user_track_s track
    join paid_user on track.uid = paid_user.user_id and substr(track.data_dt,1,7) = paid_user.mon
    where pid = '1'
        and substr(data_dt,1,7) between {{start_mon}} and {{end_mon}} 
        and substr(data_dt,9,10) < {{day}}
    group by 1,2,3
)
, group_history as (
    select *
    from (
        select
            coalesce(groupon.user_id,d.uid) as user_id
            , max(if(group_status = 1, date_format(order_info.created_time, '%Y-%m'))) as success_mon
        from bdp_dw.dw_s_suyang_user_group_buy_s groupon
        left join bdp_dw.dw_o_center_order_s order_info on order_info.id = groupon.order_id
        left join bdp_dim.dim_channel_info_s channel_info on order_info.channel_id = channel_info.channel_id
        left join (
          select device_id
            ,cast(uid as integer) uid  
          from(
            select device_id,
            uid,
            row_number()over(partition by device_id order by timestamp desc) rnn 
            from bdp_mid_as.dm_deviceid_match_uid 
            ) 
            where rnn = 1 ) d on groupon.user_id = d.uid
        where order_info.status not in (0,850)
            and identity = 0 -- 0：团长；1：团员
            and channel_info.channel_third in ('主课时拼团')
        group by 1
        )
    where success_mon is not null
)
, founder as (
    select
        date_format(date(order_info.created_time), '%Y-%m') as mon,
        group_id,
        group_status, -- 团状态 0：未成团；1：已成团
        groupon.user_id
    from bdp_dw.dw_s_suyang_user_group_buy_s groupon
        left join bdp_dw.dw_o_center_order_s order_info
            on order_info.id = groupon.order_id
        left join bdp_dim.dim_channel_info_s channel_info
            on order_info.channel_id= channel_info.channel_id
    where order_info.status not in (0,850)
        and channel_info.channel_third in ('主课时拼团')
        and date_format(order_info.created_time,'%Y-%m') between {{start_mon}} and {{end_mon}}
        and date_format(order_info.created_time,'%d') < {{day}}
        and identity = 0
    group by 1,2,3,4
)
, groupon as (
    select
        date_format(date(order_info.created_time), '%Y-%m') as mon,
        group_id,
        group_status, -- 团状态 0：未成团；1：已成团
        case when identity = 0 then '团长'-- 身份 0：团长；1：团员
            when identity = 1 then '团员'
            end as identity,
        groupon.user_id
    from bdp_dw.dw_s_suyang_user_group_buy_s groupon
        left join bdp_dw.dw_o_center_order_s order_info
            on order_info.id = groupon.order_id
        left join bdp_dim.dim_channel_info_s channel_info
            on order_info.channel_id= channel_info.channel_id
    where order_info.status not in (0,850)
        and channel_info.channel_third in ('主课时拼团')
        and date_format(order_info.created_time,'%Y-%m') between {{start_mon}} and {{end_mon}}
        and date_format(order_info.created_time,'%d') < {{day}}
    group by 1,2,3,4,5
)
, share as ( -- 埋点口径
    select
        date_format(date(data_dt), '%Y-%m') as mon,
        uid as user_id,
        share_code
    from bdp_dw.dw_u_user_track_s track
        left join bdp_dim.dim_user_info_s user_info
            on track.uid = user_info.user_id
    where substr(data_dt,1,7) between {{start_mon}} and {{end_mon}} 
        and substr(data_dt,9,10) < {{day}}
        and page_code in ('A44','B125')
        and func_code in ('A44-02','B125-01')
        and uid is not null
    group by 1,2,3
)
,eff_share as (
    select
        date_format(date(data_dt), '%Y-%m') as mon,
        cast(json_extract(ext,'$.fu') as varchar) as share_code -- json_extract(ext,'$.fu')返回的是share_code
    from bdp_dw.dw_u_user_track_s
    where substr(data_dt,1,7) between {{start_mon}} and {{end_mon}} 
        and substr(data_dt,9,10) < {{day}}
        and page_code = 'A44'
        and func_code <> 'A44-54'
        and json_extract(ext,'$.group_id') is not null --筛取参团页
    group by 1,2
)
,join_page as (
    SELECT
        cast(json_extract(ext,'$.group_id') as int) as group_id,
        device_id
        -- date_format(date(data_dt ),'%Y-%m') data_dt
    FROM
        bdp_dw.dw_l_trackserver_s
    where substr(data_dt,1,7) between {{start_mon}} and {{end_mon}} 
        and substr(data_dt,9,10) < {{day}}
        and page_code = 'A44'
        and func_code <> 'A44-54'
        -- and json_extract_scalar(ext,'$.group_id') <> ''
        -- and cid in( '1' ,'3')
    group by 1,2
)
,leads as (
    select
        date(leads_dmse.is_cc_touch_time) as data_dt  -- 可触达时间
        ,leads_dmd.leads_id
        ,leads_dmd.user_id
        ,leads_dmd.customer_id
        ,leads_dmd.from_user_id
        ,date_format(leads_dmse.is_cc_touch_time,'%Y-%m') as mon
        ,leads_dmd.channel_first  -- 线索注册新版一级渠道
        ,leads_dmd.channel_second  -- 线索注册新版二级渠道
        ,leads_dmd.channel_third -- 线索注册新版三级渠道
        ,channel_forth_value -- 线索注册新版4级渠道
        ,channel_five_value -- 线索注册新版5级渠道
        ,channel_six_value -- 线索注册新版5级渠道

        ,leads_dmse.first_call_time
        --  ,leads_dmse.first_call_status
        ,leads_dmse.first_connected_time
        ,leads_dmse.first_appointed_time
        ,leads_dmse.first_open_time
        ,leads_dmse.first_join_time
        ,leads_dmse.first_attend_time
        ,leads_dmse.first_valid_finish_time
         ,leads_dmse.first_order_time
        ,case phone_city_level when '1' then '一线城市'
                                when '2' then '新一线城市'
                                when '3' then '二线城市'
                                when '4' then '三线城市'
                                when '5' then '四线城市及以下'
                                else '未知' end city_level
    from bdp_dmd.dmd_leads_s leads_dmd
    join(
            select
                -- date(data_dt) as data_dt,
                leads_id,
                is_cc_touch_time,
                first_call_time,
                first_connected_time,
                first_appointed_time,
                first_open_time,
                first_join_time,
                first_attend_time,
                first_valid_finish_time,
                first_order_time
            from bdp_dmse.dmse_leads_h
            where is_cc_touch = 1  -- 限定都为可触达线索
                -- and date(data_dt) in (current_date, date_add('month', -1, current_date)
                and date(data_dt) = current_date
            group by 1,2,3,4,5,6,7,8,9,10
    )leads_dmse on leads_dmd.leads_id = leads_dmse.leads_id
    left join bdp_dim.dim_channel_info_s channel_info
        on leads_dmd.init_channel_id = channel_info.channel_id
    where leads_dmd.subject = 1  -- 限定科目为数学，新增
        and leads_dmd.channel_third = '主课时拼团'
        and is_overseas = 0  -- 不含海外线索
        and date_format(leads_dmse.is_cc_touch_time,'%Y-%m') between {{start_mon}} and {{end_mon}}
        and date_format(leads_dmse.is_cc_touch_time,'%d') < {{day}}
)
,info as (
select
    paid_user.mon as "月份",
    paid_user.lifecycle as "生命周期",
    count(distinct paid_user.user_id) as "付费用户数",
    count(distinct mau_paid_user.user_id) as "月活用户数",
    count(distinct if(paid_user.mon > success_mon, group_history.user_id)) as "本月无资格拼团用户数",
    count(distinct if(identity = '团长', groupon.user_id, null)) "团长人数",
    count(distinct if(identity = '团员', groupon.user_id, null)) "团员人数",
    count(distinct if(identity in ('团员','团长'), groupon.user_id, null)) "团长团员去重人数",
    count(distinct if(identity in ('团员','团长'), share.user_id, null)) "分享人数",
    count(distinct eff_share.share_code) as "有效分享人数",
    count(distinct join_page.device_id) as "参团页访问DV",
    count(distinct if(groupon.group_status = 1 and groupon.identity = '团长', groupon.user_id, null)) "成团数量",
    count(distinct leads_id) as "leads量",
    count(distinct if(month(first_appointed_time) = month(leads.data_dt), leads_id)) as "邀约量",
    count(distinct if(month(first_open_time) = month(leads.data_dt), leads_id)) as "应出席量",
    count(distinct if(month(first_attend_time) = month(leads.data_dt), leads_id)) as "出席量",
    count(distinct if(month(first_valid_finish_time) = month(leads.data_dt), leads_id)) as "完课量",
    count(distinct if(month(first_order_time) = month(leads.data_dt), leads_id)) as "订单量"
from paid_user 
left join group_history on paid_user.user_id = group_history.user_id
left join mau_paid_user on paid_user.mon = mau_paid_user.mon and paid_user.user_id = mau_paid_user.user_id
left join share on mau_paid_user.mon = share.mon and mau_paid_user.user_id = share.user_id
left join eff_share on share.mon = eff_share.mon and share.share_code = eff_share.share_code
left join founder on mau_paid_user.mon = founder.mon and mau_paid_user.user_id = founder.user_id
left join join_page on join_page.group_id = founder.group_id
left join groupon on founder.mon = groupon.mon and founder.group_id = groupon.group_id
left join leads on groupon.user_id = leads.user_id
group by
    1,2
)

select
    "月份",
    "生命周期",
    "付费用户数",
    "月活用户数",
    "本月无资格拼团用户数",
    "团长人数",
    "参团页访问DV",
    "团员人数",
    "团长团员去重人数",
    "分享人数",
    "有效分享人数",
    "成团数量",
    "leads量",
    1 - coalesce(try(1.0000 * "本月无资格拼团用户数" / "月活用户数"),0) "未拼团成功率",
    coalesce(try(1.0000 * "月活用户数"/"付费用户数"),0) "月活率",
    coalesce(try(1.0000 * "团长人数"/"月活用户数"),0) "团长/月活",
    if("团长团员去重人数" <> 0, 1.000 * "分享人数" / "团长团员去重人数") as "分享率",
    if("分享人数" <> 0, 1.000 * "有效分享人数" / "分享人数") as "有效分享率",
    if("有效分享人数" <> 0, 1.000 * "参团页访问DV" / "有效分享人数") as "人均DV",
    if("参团页访问DV" <> 0, 1.000 * "leads量" / "参团页访问DV") as "试听课领取率",
    if("团长人数" <> 0, 1.000 * "团员人数" / "团长人数") as "人均带团员量",
    if("参团页访问DV" <> 0, 1.000 * "团员人数" / "参团页访问DV") as "参团率",
    "邀约量",
    "应出席量",
    "出席量",
    "完课量",
    "订单量"
from info
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27
order by 1 desc, 2
