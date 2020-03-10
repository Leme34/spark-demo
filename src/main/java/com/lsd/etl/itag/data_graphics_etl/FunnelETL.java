package com.lsd.etl.itag.data_graphics_etl;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.lsd.etl.itag.util.SparkETLUtils;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 漏斗图ETL
 * 生产环境可以是对某一商品进行ETL
 * <p>
 * Created by lsd
 * 2020-03-05 09:09
 */
@Component
public class FunnelETL {

    @Autowired
    private Gson gson;
    @Autowired
    private SparkSession session;

//    public static void main(String[] args) {
//        System.out.println("FunnelETL = " + gson.toJson(funnel()));
//    }


    /**
     * 漏斗图ETL
     */
    public FunnelVo funnel() {
        // 测试环境模拟 展现人数,1000 点击人数,800 加入购物车人数,600
        // 真实环境应该是加入检测用户行为模块收集的数据

        //下单人数
        Dataset<Row> orderMember = session.sql(
                "select distinct(member_id)" +
                        " from i_order.t_order" +
                        " where order_status=2"
        );
        //复购人数
        Dataset<Row> orderAgainMember = session.sql(
                "select member_id" +
                        " from i_order.t_order" +
                        " where order_status=2" +
                        " group by member_id" +
                        " having count(order_id)>1"
        );
        //充值过优惠券人数
        Dataset<Row> chargeMember = session.sql(
                "select distinct(member_id) as member_id" +
                        " from i_marketing.t_coupon_member" +
                        " where coupon_channel = 1"    //coupon_channel = 1：用户购买的券
        );
        // 根据会员id进行内连接，得到复购会员中充值过优惠券的人数
        Dataset<Row> orderAgainAndChargeMember = chargeMember.join(orderAgainMember,
                orderAgainMember.col("member_id").equalTo(chargeMember.col("member_id")),
                "inner");


        long order = orderMember.count();
        long orderAgain = orderAgainMember.count();
        long chargeCoupon = orderAgainAndChargeMember.count();

        return new FunnelVo()
                .setPresent(1000L)
                .setClick(800L)
                .setAddCart(600L)
                .setOrder(order)
                .setOrderAgain(orderAgain)
                .setChargeCoupon(chargeCoupon);
    }


    /**
     * 漏斗图Vo
     */
    @Accessors(chain = true)
    @Data
    private static class FunnelVo {
        private Long present;       // 展现：打开APP人数
        private Long click;         // 点击：点击任意商品人数
        private Long addCart;       // 加购：将任意商品加入购物车的人数
        private Long order;         // 下单：下单人数
        private Long orderAgain;    // 复购：超过一次下单记录的人数
        private Long chargeCoupon;  // 储值：复购的会员中充值过优惠券的人数
    }

}
