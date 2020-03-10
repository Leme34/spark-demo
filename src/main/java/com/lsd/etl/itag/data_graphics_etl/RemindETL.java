package com.lsd.etl.itag.data_graphics_etl;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.lsd.etl.itag.util.DateUtils;
import com.lsd.etl.itag.util.SparkETLUtils;
import lombok.Data;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.Month;
import java.util.Date;
import java.util.List;

/**
 * 用户抵扣券到期前 1 天未使用提醒功能
 * 约定系统的优惠券是 8 天失效的
 * <p>
 * Created by lsd
 * 2020-03-04 17:28
 */
@Component
public class RemindETL {

    @Autowired
    private SparkSession session;
    @Autowired
    private SparkETLUtils sparkETLUtils;

//    public static void main(String[] args) {
//        System.out.println("freeReminderList = " + gson.toJson(freeReminderList()));
//        System.out.println("couponReminders = " + gson.toJson(couponReminders()));
//    }

    /**
     * 最近一周"首单免费优惠券"的领券情况
     *
     * @return 前n天(n < = 7)当天领取"首单免费优惠券"的人数
     */
    public List<FreeReminder> freeReminderList() {
        // 测试数据的日期最新时间是2019.10.30
        LocalDate now = LocalDate.of(2019, Month.NOVEMBER, 30);
        Date nowDate = DateUtils.localDate2Date(now);
        // 约定的系统优惠券是 8 天失效的，明天失效的券领券日期是7天前
        Date pickDate = DateUtils.addDateDays(nowDate, -7);
        String sql = String.format(
                "select date_format(create_time,'yyyy-MM-dd') as day,count(member_id) as freeCount" +
                        " from i_marketing.t_coupon_member" +
                        " where coupon_id = 1" +       //coupon_id = 1的是系统预先设定好的首单优惠券
                        " and coupon_channel = 2" +    //领券渠道为公司发放
                        " and create_time >= '%s'" +   //领取日期是7天前
                        " group by date_format(create_time,'yyyy-MM-dd')",
                DateUtils.format(pickDate, DateUtils.DATE_TIME_PATTERN)
        );
        return sparkETLUtils.execAndCollectAsList(session, sql, FreeReminder.class);
    }

    /**
     * 最近一周"普通优惠券"的领券情况
     *
     * @return 前n天(n < = 7)当天领取"非首单免费优惠券"的人数
     */
    public List<CouponReminder> couponReminders() {
        // 测试数据的日期最新时间是2019.10.30
        LocalDate now = LocalDate.of(2019, Month.NOVEMBER, 30);
        Date nowDate = DateUtils.localDate2Date(now);
        // 约定的系统优惠券是 8 天失效的，明天失效的券领券日期是7天前
        Date pickDate = DateUtils.addDateDays(nowDate, -7);
        String sql = String.format(
                "select date_format(create_time,'yyyy-MM-dd') as day,count(member_id) as couponCount" +
                        " from i_marketing.t_coupon_member" +
                        " where coupon_id != 1" +       //除系统预先设定好的"首单优惠券"外的优惠券
                        " and create_time >= '%s'" +    //领取日期是7天前
                        " group by date_format(create_time,'yyyy-MM-dd')",
                DateUtils.format(pickDate, DateUtils.DATE_TIME_PATTERN)
        );
        return sparkETLUtils.execAndCollectAsList(session, sql, CouponReminder.class);
    }


    /**
     * 最近一周"首单免费优惠券"的领券情况
     */
    @Data
    private static class FreeReminder {
        private String day;             //日期
        private Integer freeCount;      //当天领取"首单免费优惠券"的用户数量
    }

    /**
     * 最近一周"普通优惠券"的领券情况
     */
    @Data
    private static class CouponReminder {
        private String day;             //日期
        private Integer couponCount;    //当天领取"优惠券"的用户数量
    }

}
