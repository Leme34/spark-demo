package com.lsd.etl.itag.data_graphics_etl;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.lsd.etl.itag.util.SparkETLUtils;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Spark操作Hive实现全部用户热度数据的ETL
 * <p>
 * Created by lsd
 * 2020-03-03 21:26
 */
public class UserHeatETL {

    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
    private static final SparkSession session = SparkETLUtils.initSparkSession4Hive();

    public static void main(String[] args) {
        MemberVo vo = new MemberVo()
                .setMemberSexes(memberSex())
                .setMemberChannels(memberRegChannel())
                .setMemberMpSubs(memberMpSubscribe())
                .setMemberHeat(memberHeat());
        System.out.println("ETL Result = " + gson.toJson(vo));
    }

    /**
     * 按性别分组统计
     * -1:未知 1:男 2:女
     */
    public static List<MemberSex> memberSex() {
        String sql = "select sex as memberSex,count(id) as sexCount from i_member.t_member group by sex";
        return SparkETLUtils.execAndCollectAsList(session, sql, MemberSex.class);
    }

    /**
     * 按会员注册渠道分组统计
     * 1:IOS 2:android 3:微信小程序 4:微信公众号 5:h5
     */
    public static List<MemberChannel> memberRegChannel() {
        String sql = "select member_channel as memberChannel, count(id) as channelCount from i_member.t_member group by member_channel";
        return SparkETLUtils.execAndCollectAsList(session, sql, MemberChannel.class);
    }

    /**
     * 按 是否关注了微信公众号（mp_open_id是否为空） 分组统计
     * 注意默认情况下Sqoop导入的null会变为"null"
     */
    public static List<MemberMpSub> memberMpSubscribe() {
        String sql = "select count(if(mp_open_id !='null',id,null)) as subCount," +  //已关注微信公众号
                "count(if(mp_open_id ='null',id,null)) as unSubCount " +    //未关注微信公众号
                "from i_member.t_member";
        return SparkETLUtils.execAndCollectAsList(session, sql, MemberMpSub.class);
    }

    /**
     * 用户热度信息聚合查询
     * <p>
     * reg:        已注册的用户，目标表：i_member.t_member，条件：phone = 'null'
     * complete:   已完善信息的用户，目标表：i_member.t_member，条件：phone != 'null'
     * order:      已下过单的用户，目标表：i_order.t_order，条件：t.orderCount = 1
     * orderAgain: 有回购记录的用户，目标表：i_order.t_order，条件：t.orderCount >= 2
     * coupon:     有领券记录的用户，目标表：i_marketing.t_coupon_member，条件：count(distinct member_id)
     */
    public static MemberHeat memberHeat() {
        Dataset<Row> reg_complete = session.sql(
                "select count(if(phone='null',id,null)) as reg," +
                        "count(if(phone !='null',id,null)) as complete" +
                        " from i_member.t_member"
        );
        Dataset<Row> order_again = session.sql(
                "select count(if(t.orderCount = 1,t.member_id,null)) as order," +
                        "count(if(t.orderCount >= 2,t.member_id,null)) as orderAgain from" +
                        " (select count(order_id) as orderCount,member_id from i_order.t_order group by member_id) as t"
        );
        Dataset<Row> coupon = session.sql(
                "select count(distinct member_id) as coupon from i_marketing.t_coupon_member"
        );

        // 生产环境慎用笛卡尔积，效率非常低下，此处用于测试偷懒
        Dataset<Row> resultDataset = coupon.crossJoin(reg_complete).crossJoin(order_again);
        List<MemberHeat> resultJsons = resultDataset.toJSON().collectAsList()
                .stream()
                .map(str -> gson.fromJson(str, MemberHeat.class))
                .collect(Collectors.toList());
        return resultJsons.get(0);
    }


    @Data
    private static class MemberSex {
        private Integer memberSex;
        private Integer sexCount;
    }

    @Data
    private static class MemberChannel {
        private Integer memberChannel;
        private Integer channelCount;
    }

    @Data
    private static class MemberMpSub {
        private Integer subCount;
        private Integer unSubCount;
    }


    /**
     * 用户热度聚合信息
     */
    @Data
    private static class MemberHeat {
        private Integer reg;        //已注册的用户数量
        private Integer complete;   //已完善信息的用户数量
        private Integer order;      //已下过单的用户数量
        private Integer orderAgain; //有回购记录的用户数量
        private Integer coupon;     //有领券记录的用户数量
    }

    /**
     * 用户ETL结果Vo
     */
    @Accessors(chain = true)
    @Data
    private static class MemberVo {
        private List<MemberSex> memberSexes;
        private List<MemberChannel> memberChannels;
        private List<MemberMpSub> memberMpSubs;
        private MemberHeat memberHeat;
    }

}
