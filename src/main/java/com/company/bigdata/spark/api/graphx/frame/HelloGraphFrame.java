package com.company.bigdata.spark.api.graphx.frame;

import com.company.bigdata.spark.api.graphx.entity.Relation;
import com.company.bigdata.spark.api.graphx.entity.User;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HelloGraphFrame {

    private final SparkSession sparkSession;
    private final SQLContext sqlContext;
    private final JavaSparkContext javaSparkContext;

    public HelloGraphFrame(){
        SparkConf sparkConf = new SparkConf()
                .setAppName("SparkGraphFrames")
                .setMaster("local[*]");
        javaSparkContext = new JavaSparkContext(sparkConf);
        sparkSession = SparkSession.builder()
                .appName("SparkGraphFrameSample")
                .sparkContext(javaSparkContext.sc())
                .master("local[*]")
                .getOrCreate();
        sqlContext = new SQLContext(javaSparkContext);
    }

    public void construct(){
        List<User> uList = new ArrayList<User>() {
            {
                add(new User("a", "Alice", 34));
                add(new User("b", "Bob", 36));
                add(new User("c", "Charlie", 30));
            }
        };

        Dataset<Row> verDF = sparkSession.createDataFrame(uList, User.class);

        //Create an Edge DataFrame with "src" and "dst" columns
        List<Relation> rList = new ArrayList<Relation>() {
            {
                add(new Relation("a", "b", "friend"));
                add(new Relation("b", "c", "follow"));
                add(new Relation("c", "b", "follow"));
            }
        };

        Dataset<Row> edgDF = sparkSession.createDataFrame(rList, Relation.class);

        //Create a GraphFrame : err but run :))
        GraphFrame gFrame =new GraphFrame(verDF, edgDF);
        gFrame.inDegrees().show();
        gFrame.vertices().show();

    }

    public void constructv2(){


        JavaRDD<Row> verRow =
                javaSparkContext.parallelize(Arrays.asList(RowFactory.create(101L,"Trina",27),
                        RowFactory.create(201L,"Raman",45),
                        RowFactory.create(301L,"Ajay",32),
                        RowFactory.create(401L,"Sima",23)));
        List<StructField> verFields = new ArrayList<StructField>();
        verFields.add(DataTypes.createStructField("id",DataTypes.LongType, true));
        verFields.add(DataTypes.createStructField("name",DataTypes.StringType, true));
        verFields.add(DataTypes.createStructField("age",DataTypes.IntegerType, true));

        JavaRDD<Row> edgRow =javaSparkContext.parallelize(Arrays.asList(
                RowFactory.create(101L,301L,"Colleague"),
                RowFactory.create(101L,401L,"Friends"),
                RowFactory.create(401L,201L,"Reports"),
                RowFactory.create(301L,201L,"Reports"),
                RowFactory.create(201L,101L,"Reports")));
        List<StructField> EdgFields = new ArrayList<StructField>();
        EdgFields.add(DataTypes.createStructField("src",DataTypes.LongType, true));
        EdgFields.add(DataTypes.createStructField("dst",DataTypes.LongType, true));
        EdgFields.add(DataTypes.createStructField("relationType",DataTypes.StringType, true));

        StructType verSchema = DataTypes.createStructType(verFields);
        StructType edgSchema = DataTypes.createStructType(EdgFields);

        Dataset<Row> verDF = sqlContext.createDataFrame(verRow, verSchema);
        Dataset<Row> edgDF = sqlContext.createDataFrame(edgRow, edgSchema);

        GraphFrame g = new GraphFrame(verDF,edgDF);
        g.vertices().show();
        g.vertices().groupBy().min("age").show();
    }

    public void construct3(){

    }


    public static void main(String[] args) {
        // construct 1, 2 is graph frame
        new HelloGraphFrame().construct();
    }


}

