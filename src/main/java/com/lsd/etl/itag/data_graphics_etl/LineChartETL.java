package com.lsd.etl.itag.data_graphics_etl;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.lsd.etl.itag.util.DateUtils;
import com.lsd.etl.itag.util.SparkETLUtils;
import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import scala.Tuple2;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.Month;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 订单GMV折线图ETL
 * <p>
 * Created by lsd
 * 2020-03-04 20:29
 */
public class LineChartETL {

    @Autowired
    private Gson gson;
    @Autowired
    private SparkSession session;

//    public static void main(String[] args) {
//        System.out.println("LineChartETL = " + gson.toJson(lineVos()));
//    }

    /**
     * ETL求得每天的折线图Vo数据
     *
     * @return 周一~七的折线图Vo数据
     */
    public List<LineVo> lineVos() {
        LocalDate now = LocalDate.of(2019, Month.NOVEMBER, 30);// 测试数据的日期最新时间是2019.10.30
        Date nowDate = DateUtils.localDate2Date(now);
        Date sevenDaysBefore = DateUtils.addDateDays(nowDate, -8);

        //当日注册量和当日为止的会员总量
        String reg_memberSQL = String.format(
                "select date_format(create_time,'yyyy-MM-dd') as day," +
                        " count(id) as regCount," +    //当天注册量
                        " max(id) as memberCount" +    //因为id自增，所以max(id)就是当天为止的会员总量
                        " from i_member.t_member where create_time >='%s'" +
                        " group by date_format(create_time,'yyyy-MM-dd')" +
                        " order by day",
                DateUtils.format(sevenDaysBefore, DateUtils.DATE_TIME_PATTERN)
        );
        final Dataset<Row> memberDs = session.sql(reg_memberSQL);

        //当日为止的订单总数和当日GMV
        String order_gmvSQL = String.format(
                "select date_format(create_time,'yyyy-MM-dd') as day," +
                        " max(order_id) orderCount," + //因为order_id自增，所以max(order_id)就是当天为止的订单总量
                        " sum(origin_price) as gmv" +  //当日GMV
                        " from i_order.t_order where create_time >='%s'" +
                        " group by date_format(create_time,'yyyy-MM-dd')" +
                        " order by day",
                DateUtils.format(sevenDaysBefore, DateUtils.DATE_TIME_PATTERN)
        );
        final Dataset<Row> orderDs = session.sql(order_gmvSQL);

        // 两个表根据day关联：memberDs inner join orderDs on a.day = b.day
        final Dataset<Tuple2<Row, Row>> tuple2Dataset = memberDs.joinWith(orderDs,
                memberDs.col("day").equalTo(orderDs.col("day")),
                "inner");
        List<Tuple2<Row, Row>> tuple2s = tuple2Dataset.collectAsList();

        // 查询结果转换LineVo对象
        List<LineVo> lineVos = tuple2s.stream()
                .map(tuple2 -> {
                    //因为不知道Hive查出来的结果集中每个字段名字及其类型，所以需要借助Json对象转换
                    JsonObject jsonObject = new JsonObject();
                    Row row1 = tuple2._1();
                    String[] fieldNames = row1.schema().fieldNames();
                    for (String fieldName : fieldNames) {
                        jsonObject.add(fieldName, gson.toJsonTree(row1.getAs(fieldName)));
                    }
                    Row row2 = tuple2._2();
                    fieldNames = row2.schema().fieldNames();
                    for (String fieldName : fieldNames) {
                        jsonObject.add(fieldName, gson.toJsonTree(row2.getAs(fieldName)));
                    }
                    return gson.fromJson(jsonObject, LineVo.class);
                }).collect(Collectors.toList());

        // 截止至7天前的总gmv
        String gmvTotalSQL = String.format(
                "select sum(origin_price) as totalGmv from i_order.t_order where create_time <'%s'",
                DateUtils.format(sevenDaysBefore, DateUtils.DATE_TIME_PATTERN)
        );
        Dataset<Row> gmvDs = session.sql(gmvTotalSQL);
        BigDecimal gmvTotal = BigDecimal.valueOf(gmvDs.collectAsList().get(0).getDouble(0));

        // 数组元素的值 = 截止至数组下标那天的总gmv
        List<BigDecimal> gmvTotalByDay = new ArrayList<>();
        for (int i = 0; i < lineVos.size(); i++) {
            // 第i天当日gmv累加截止至7天前的总gmv
            BigDecimal temp = lineVos.get(i).getGmv().add(gmvTotal);
            // 使 temp = 截止至第i天总的gmv
            for (int j = 0; j < i; j++) {
                temp = temp.add(lineVos.get(j).getGmv());
            }
            gmvTotalByDay.add(temp);
        }
        // set每日gmv总量到每天的折线图Vo中
        for (int i = 0; i < gmvTotalByDay.size(); i++) {
            lineVos.get(i).setGmv(gmvTotalByDay.get(i));
        }
        return lineVos;
    }

    /**
     * 折线图Vo
     */
    @Data
    private static class LineVo {
        private String day;          //周几（周一~七）
        private Integer regCount;    //注册量
        private Integer memberCount; //会员总量
        private Integer orderCount;  //订单数
        //截止到当天的GMV（订单商品总金额=商品原价*商品数量）
        private BigDecimal gmv;
    }

}
