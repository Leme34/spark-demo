package com.lsd.etl.itag.util;

import com.lsd.etl.itag.util.SparkETLUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.JoinType;
import org.apache.spark.sql.catalyst.plans.logical.Join;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.hive.MetastoreRelation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scala.Option;
import scala.collection.Seq;

import javax.annotation.PostConstruct;

/**
 * Spark SQL的执行过程分析
 * <p>
 * Created by lsd
 * 2020-03-09 21:51
 */
@Component
public class SparkExplainService {

    @Autowired
    private SparkSession session;


    public void explain(String sql) {
        Dataset<Row> dataset = session.sql(sql);
        // 获取Logical Plan（逻辑执行计划）
        LogicalPlan logicalPlan = dataset.logicalPlan();
        String logicalPlanStr = logicalPlan.toString();
        boolean analyzed = logicalPlan.analyzed();//此节点及其子节点已经过分析和验证，则返回true
        // 从Logical Plan分析数据血缘（表的元信息）
        LogicalPlan node0 = logicalPlan.p(0);//返回第0个树节点，主要用于交互式调试。
        // 遍历每个树节点
        int i = 0;
        while (true) {
            final LogicalPlan node = logicalPlan.p(i);
            if (node == null) {
                break;
            }
            // 表的元数据节点
            if (node instanceof MetastoreRelation) {
                MetastoreRelation m = (MetastoreRelation) node;
                String tableName = m.tableName();
                String databaseName = m.databaseName();
            }
            // 若是Join操作节点
            if (node instanceof Join) {
                Join join = (Join) node;
                //Join的条件
                Option<Expression> condition = join.condition();
                JoinType joinType = join.joinType();
            }
            i++;
        }

        // 获取catalog，获取catalog中包含了Hive的元信息
        SessionCatalog catalog = dataset.sparkSession().sessionState().catalog();
        Seq<String> databases = catalog.listDatabases();
        Seq<TableIdentifier> tables = catalog.listTables("i_order");


    }


}
