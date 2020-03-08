package com.lsd.etl.demo;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 使用Spark操作ES例子
 * <p>
 * Created by lsd
 * 2020-03-05 14:39
 */
public class SparkEsDemo {

    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("SparkEsDemo")
                .setMaster("spark://spark-master:7077") //提交到Spark执行
                .setJars(new String[]{"/spark-demo1-1.0-SNAPSHOT.jar"})  //设置分发到集群的jar，非local模式必须配置否则ClassCastException
                .set("es.nodes", "elasticsearch")
                .set("es.port", "9200")
                .set("es.index.auto.create", "true");  // 若索引mapping结构不存在则自动创建
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        // 自动建索引以及插入示例
//        autoCreateIndex(sparkContext);

        // 查询索引示例
//        queryAll(sparkContext);

        // 条件查询
        boolQuery(sparkContext);
    }

    /**
     * 条件查询
     */
    private static void boolQuery(JavaSparkContext sparkContext) {
        String queryJson = "{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"should\": [\n" +
                "        {\n" +
                "          \"match\": {\n" +
                "            \"name\": \"lsd\"\n" +
                "          }\n" +
                "        },\n" +
                "        {\n" +
                "          \"range\": {\n" +
                "            \"age\": {\n" +
                "              \"gte\": 20,\n" +
                "              \"lte\": 30\n" +
                "            }\n" +
                "          }\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  }\n" +
                "}";
        JavaPairRDD<String, String> pairRDD = JavaEsSpark.esJsonRDD(sparkContext, "/user/_doc", queryJson);
        // 打印Spark查出ES数据的结构：
        //  {
        //    "QXf-pHABGP5_6U_ORWRU": "{\"age\":18,\"name\":\"lsd\"}"
        //  }
        Map<String, String> stringStringMap = pairRDD.collectAsMap();
        System.out.println(gson.toJson(stringStringMap));
        final JavaRDD<User> rdd = pairRDD.map(new Function<Tuple2<String, String>, User>() {
            @Override
            public User call(Tuple2<String, String> v1) throws Exception {
                return gson.fromJson(v1._2, User.class);
            }
        });
        List<User> userList = rdd.collect();
        System.out.println(gson.toJson(userList));
    }

    /**
     * 自动建索引以及插入示例
     */
    private static void autoCreateIndex(JavaSparkContext sparkContext) {
        List<User> users = new ArrayList<>();
        users.add(new User("lsd", 18));
        users.add(new User("lee", 19));
        final JavaRDD<User> userJavaRDD = sparkContext.parallelize(users);
        JavaEsSpark.saveToEs(userJavaRDD, "/user/_doc");
    }

    /**
     * 查询索引示例
     */
    private static void queryAll(JavaSparkContext sparkContext) {
        JavaPairRDD<String, Map<String, Object>> pairRDD = JavaEsSpark.esRDD(sparkContext, "/user/_doc");
        // 打印Spark查出ES数据的结构：
        //  {
        //    "QHf-pHABGP5_6U_OLmQc": {
        //      "age": 19,
        //      "name": "lee"
        //    },
        //    "QXf-pHABGP5_6U_ORWRU": {
        //      "age": 18,
        //      "name": "lsd"
        //    }
        //  }
        Map<String, Map<String, Object>> stringMapMap = pairRDD.collectAsMap();
        System.out.println(gson.toJson(stringMapMap));

        // 映射为User结构
        JavaRDD<User> rdd = pairRDD.map(new Function<Tuple2<String, Map<String, Object>>, User>() {
            @Override
            public User call(Tuple2<String, Map<String, Object>> v1) throws Exception {
                User user = new User();
                // 把map的键值映射到user对象的属性中
                BeanUtils.populate(user, v1._2);
                return user;
            }
        });
        List<User> userList = rdd.collect();
        System.out.println(gson.toJson(userList));
    }


    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class User implements Serializable {
        private static final long serialVersionUID = -143043078766513103L;
        private String name;
        private Integer age;
    }


}
