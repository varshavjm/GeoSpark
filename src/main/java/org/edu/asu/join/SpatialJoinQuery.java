package org.edu.asu.join;

import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.RectangleRDD;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;

import scala.Tuple2;

import org.datasyslab.geospark.spatialOperator.JoinQuery;
import org.datasyslab.geospark.spatialOperator.RangeQuery;

public class SpatialJoinQuery {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		getSpatialJoinQuery();
		}
		
		
		public static JavaPairRDD<Envelope, HashSet<Point>> getSpatialJoinQuery()
		{
			
			SparkConf conf = new SparkConf().setMaster("local").setAppName("JoinQuery");
			final JavaSparkContext sc = new JavaSparkContext(conf);

			PointRDD objectRDD = new PointRDD(sc, "V:\\DDS\\arealm.csv", 0, FileDataSplitter.CSV, false, 10, StorageLevel
					.MEMORY_ONLY_SER()); /*
											 * The O means spatial attribute starts at
											 * Column 0 and the 10 means 10 RDD
											 * partitions
											 */
			RectangleRDD rectangleRDD = new RectangleRDD(sc, "V:\\DDS\\zcta510.csv", 0, FileDataSplitter.CSV, false,
					StorageLevel
							.MEMORY_ONLY_SER()); 
			// collect rectangles into a java list
			JavaRDD javaRDD = rectangleRDD.getRawSpatialRDD();
			List<Envelope>rectangleRDDList=javaRDD.collect();
			JavaRDD<Point> resultRDD = null;
			List listOfPoints=new ArrayList<Point>();
			List<Tuple2<Envelope, HashSet<Point>>>pointRectanglePairList=new ArrayList<Tuple2<Envelope, HashSet<Point>>>();
			JavaPairRDD<Envelope, HashSet<Point>> resultList = null;
			for (Envelope env : rectangleRDDList) {
				try {
					resultRDD = RangeQuery.SpatialRangeQuery(objectRDD, env, 0, false);
					listOfPoints=resultRDD.collect();
					HashSet resultPointSet=new HashSet<Point>(listOfPoints);
					Tuple2 tuple=new Tuple2<Envelope, HashSet<Point>>(env,resultPointSet);
					pointRectanglePairList.add(tuple);
					
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				resultList=sc.parallelizePairs(pointRectanglePairList);
				return resultList;
		}
			return resultList;
	}

}
