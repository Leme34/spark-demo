package com.lsd.etl.itag.data_graphics_etl;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.lsd.etl.itag.util.DateUtils;
import com.lsd.etl.itag.util.SparkETLUtils;
import lombok.Data;
import org.apache.spark.sql.SparkSession;

import java.time.LocalDate;
import java.time.Month;
import java.util.Date;
import java.util.List;

/**
 * 本周与上周用户热度数据ETL
 * <p>
 * Created by lsd
 * 2020-03-04 14:20
 */
public class WeekOnWeekETL {

    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
    private static final SparkSession session = SparkETLUtils.initSparkSession4Hive();


    public static void main(String[] args) {
        System.out.println("registerCount result = " + gson.toJson(registerCount()));
        System.out.println("orderCount result = " + gson.toJson(orderCount()));
    }

    /**
     * 最近一周（前7天到前14天）注册量ETL
     *
     * @return 每天的注册量
     */
    public static List<Reg> registerCount() {
        // 测试数据的日期最新时间是2019.10.30
        LocalDate now = LocalDate.of(2019, Month.NOVEMBER, 30);
        Date nowDate = DateUtils.localDate2Date(now);
        Date lastWeek = DateUtils.addDateDays(nowDate, -7);
        Date twoWeeksAgo = DateUtils.addDateDays(nowDate, -14);
        String sql = String.format(
                "select date_format(create_time,'yyyy-MM-dd') as day," +
                        " count(id) as regCount" +
                        " from i_member.t_member" +
                        " where create_time >='%s'" +
                        " and create_time < '%s'" +
                        " group by date_format(create_time,'yyyy-MM-dd')",
                DateUtils.format(twoWeeksAgo, DateUtils.DATE_TIME_PATTERN),
                DateUtils.format(lastWeek, DateUtils.DATE_TIME_PATTERN)
        );
        return SparkETLUtils.execAndCollectAsList(session, sql, Reg.class);
    }

    /**
     * 最近一周（前7天到前14天）订单量ETL
     *
     * @return 每天的订单量
     */
    public static List<Order> orderCount() {
        // 测试数据的日期最新时间是2019.11.30
        LocalDate now = LocalDate.of(2019, Month.NOVEMBER, 30);
        Date nowDate = DateUtils.localDate2Date(now);
        Date lastWeek = DateUtils.addDateDays(nowDate, -7);
        Date twoWeeksAgo = DateUtils.addDateDays(nowDate, -14);
        String sql = String.format(
                "select date_format(create_time,'yyyy-MM-dd') as day," +
                        " count(order_id) as orderCount" +
                        " from i_order.t_order where create_time >='%s'" +
                        " and create_time < '%s' " +
                        " group by date_format(create_time,'yyyy-MM-dd')",
                DateUtils.format(twoWeeksAgo, DateUtils.DATE_TIME_PATTERN),
                DateUtils.format(lastWeek, DateUtils.DATE_TIME_PATTERN)
        );
        return SparkETLUtils.execAndCollectAsList(session, sql, Order.class);
    }


    /**
     * 周注册量结构体
     */
    @Data
    private static class Reg {
        private String day;         //日期
        private Integer regCount;   //当天注册量
    }


    /**
     * 周订单量结构体
     */
    @Data
    private static class Order {
        private String day;         //日期
        private Integer orderCount; //当天订单量
    }

}
