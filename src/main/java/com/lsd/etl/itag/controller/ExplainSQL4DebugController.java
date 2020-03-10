package com.lsd.etl.itag.controller;

import com.lsd.etl.itag.util.SparkExplainService;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 用于 debug 分析 Spark SQL 的接口
 * <p>
 * Created by lsd
 * 2020-03-09 21:58
 */
@RestController
public class ExplainSQL4DebugController {

    @Autowired
    private SparkExplainService sparkExplainService;

    @GetMapping("explain")
    public String explain4Debug() {
        String sql =
                "SELECT o.member_id AS memberId, date_format( max( o.create_time ), 'yyyy-MM-dd' ) AS orderTime," +
                        " count( DISTINCT o.order_id ) AS orderCount," +
                        " collect_list( DISTINCT oc.commodity_id ) AS favGoods," +
                        " collect_set(sum_by_member.orderMoney)[0] AS orderMoney" +
                        " FROM i_order.t_order AS o" +
                        " LEFT JOIN (" +
                        " SELECT o.member_id, sum( o.pay_price ) AS orderMoney" +
                        " FROM i_order.t_order o" +
                        " GROUP BY o.member_id" +
                        " ) sum_by_member ON o.member_id = sum_by_member.member_id" +
                        " LEFT JOIN i_order.t_order_commodity AS oc ON o.order_id = oc.order_id" +
                        " GROUP BY o.member_id";
        sparkExplainService.explain(sql);
        return "ok";
    }


}
