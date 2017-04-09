import org.apache.spark.sql.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.Encoders;
import scala.Tuple2;
import scala.Tuple2.*;
import java.util.*;
import java.lang.*;
import com.google.common.collect.Lists;

public class Main {
    public static void main(String[] args) {
        StringBuilder d = new StringBuilder();
        final StringBuilder f[] = {d};
        final String y1[] = {""};
        final String y2[] = {""};
        final String y3[] = {""};
        final String y4[] = {""};
        final String y5[] = {""};
        final String x1[] = {""};
        final String x2[] = {""};
        final String w[] = {""}; 
        final String str1[] = {""};
        final String str2[] = {""};
        final String s[][] = {{"","","","",""}};
        final String q[][] = {{"","",""}};
        System.out.println("Hello!");
        SparkSession spark = SparkSession.builder().appName("ChainJoinOptimal").master("local[*]").getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        String dir = System.getProperty("user.dir");
        Dataset<Row> d15 = spark.read().parquet(dir + "/t1/web_sales.parquet");
        Dataset<Row> d25 = spark.read().parquet(dir + "/t1/store_sales.parquet");
        Dataset<Row> d35 = spark.read().parquet(dir + "/t1/store_returns.parquet");
        Dataset<Row> d45 = spark.read().parquet(dir + "/t1/catalog_sales.parquet");
        Dataset<Row> d1 = d15.sample(false, 0.0005), d2 = d25.sample(false, 0.0005), d3 = d35.sample(false, 0.0005), d4 = d45.sample(false, 0.0005);
        //counters for tables' sizes
        long wsLen = d1.count(), ssLen = d2.count(), srLen = d3.count(), csLen = d4.count();
        //calculations for share values, intermediate share value(a2) is approximately 1 
        final int a1 = 5;
        int a2 = 1;
        final int a3 = 2;
        //number of reducers
        int k = a1*a2*a3;
        //map function for the first table
        PairFlatMapFunction<Row, String, String> hashing1 = new PairFlatMapFunction<Row, String, String>() 
        {
            @Override
            public Iterator<Tuple2<String, String>> call(Row value) throws Exception
            {
                int h1, h2 = 1, value1, value2;
                if (value.get(1) != null)
                {
                    h1 = value.getInt(1)%a1;
                    value2 = value.getInt(1);
                }
                else
                {
                    h1 = 1;
                    value2 = 1;
                }
                if (value.get(0) != null)
                    value1 = value.getInt(0);
                else
                    value1 = 1;
                List<Tuple2<String, String>> pairs = new LinkedList<Tuple2<String, String>>();
                for (int i=0; i<a3; i++)
                {  
                    //map-key is the 1 of 3 hash values and the other are the values 1 and 0-a3
                    f[0].setLength(0);
                    f[0].append(h1);
                    f[0].append(",");
                    f[0].append(1);
                    f[0].append(",");
                    f[0].append(i);
                    str1[0] = f[0].toString();
                    //map-value is the table and the values of the table
                    f[0].setLength(0);
                    f[0].append(1);
                    f[0].append(",");
                    f[0].append(value1);
                    f[0].append(",");
                    f[0].append(value2);
                    str2[0] = f[0].toString();
                    pairs.add(new Tuple2<String, String>(str1[0] , str2[0]));
                }
                return pairs.iterator();
            }
        };
        //map function for the second table
        PairFlatMapFunction<Row, String, String> hashing2 = new PairFlatMapFunction<Row, String, String>()  
        {
            @Override
            public Iterator<Tuple2<String, String>> call(Row value) throws Exception
            {
                int h1, h2 = 1, h3, value1, value2;
                if (value.get(0) != null)
                {
                    h1 = value.getInt(0)%a1;
                    value1 = value.getInt(0);
                }
                else
                {
                    h1 = 1;
                    value1 = 1;
                }
                if (value.get(1) != null)
                {
                    h2 = 1;
                    value2 = value.getInt(1);
                }
                else
                {
                    h2 = 1;
                    value2 = 1;
                }
                List<Tuple2<String, String>> pairs = new LinkedList<Tuple2<String, String>>();
                for(int i=0 ;i<a3 ;i++)
                {
                    //map-key is the 1 of 3 hash values and the other are the values 1 and 0-a3
                    f[0].setLength(0);
                    f[0].append(h1);
                    f[0].append(",");
                    f[0].append(h2);
                    f[0].append(",");
                    f[0].append(i);
                    str1[0] = f[0].toString();
                    //map-value is the table and the values of the table
                    f[0].setLength(0);
                    f[0].append(2);
                    f[0].append(",");
                    f[0].append(value1);
                    f[0].append(",");
                    f[0].append(value2);
                    str2[0] = f[0].toString();
                    pairs.add(new Tuple2<String, String>(str1[0] , str2[0]));
                }
                return pairs.iterator();
            }
        };
        //map function for the third table
        PairFlatMapFunction<Row, String, String> hashing3 = new PairFlatMapFunction<Row, String, String>() 
        {
            @Override
            public Iterator<Tuple2<String, String>> call(Row value) throws Exception 
            {
                int h2 = 1;
                int h3;
                int value1;
                int value2;
                if (value.get(0) != null)
                {
                    long u1 = value.getLong(0);
                    value1 = (int) u1;
                }
                else
                    value1 = 1;
                if (value.get(1) != null)
                {
                    long foo = value.getLong(1)%a3;
                    long u2 = value.getLong(1);
                    h3 = (int) foo;
                    value2 = (int) u2;
                }
                else
                {
                    h3 = 1;
                    value2 = 1;
                }
                List<Tuple2<String, String>> pairs = new LinkedList<Tuple2<String, String>>();
                for(int i=0 ;i<a1 ;i++)
                {
                    //map-key is the 1 of 3 hash values and the other are the values 1 and 0-a1
                    f[0].setLength(0);
                    f[0].append(i);
                    f[0].append(",");
                    f[0].append(h2);
                    f[0].append(",");
                    f[0].append(h3);
                    str1[0] = f[0].toString();
                    //map-value is the table and the values of the table
                    f[0].setLength(0);
                    f[0].append(3);
                    f[0].append(",");
                    f[0].append(value1);
                    f[0].append(",");
                    f[0].append(value2);
                    str2[0] = f[0].toString();
                    pairs.add(new Tuple2<String, String>(str1[0] , str2[0]));
                }
                return pairs.iterator();
            }
        };
        //map function for the fourth table
        PairFlatMapFunction<Row, String, String> hashing4 = new PairFlatMapFunction<Row, String, String>() 
        {
            @Override
            public Iterator<Tuple2<String, String>> call(Row value) throws Exception
            {
                int h3, value1, value2;
                if (value.get(0) != null)
                {
                    h3 = value.getInt(0)%a3;
                    value1 = value.getInt(0);
                }
                else
                {
                    h3 = 1;
                    value1 = 1;
                }
                if (value.get(1) != null)
                    value2 = value.getInt(1);
                else
                    value2 = 1;
                List<Tuple2<String, String>> pairs = new LinkedList<Tuple2<String, String>>();
                for (int i=0; i<a1; i++)
                {
                    //map-key is the 1 of 3 hash values and the other are the values 1 and 0-a1
                    f[0].setLength(0);
                    f[0].append(i);
                    f[0].append(",");
                    f[0].append(1);
                    f[0].append(",");
                    f[0].append(h3);
                    str1[0] = f[0].toString();
                    //map-value is the table and the values of the table
                    f[0].setLength(0);
                    f[0].append(4);
                    f[0].append(",");
                    f[0].append(value1);
                    f[0].append(",");
                    f[0].append(value2);
                    str2[0] = f[0].toString();
                    pairs.add(new Tuple2<String, String>(str1[0], str2[0]));
                }
                return pairs.iterator();
            }
        };
        //reduce phase(join at each reducer), through a mapValues function
        Function<Iterable<String>, Iterable<String>> reduce = new Function<Iterable<String>, Iterable<String>>()
        {
            @Override
            public Iterable<String> call(Iterable<String> oldList)
            {
                List<String> newTempList = new ArrayList<String>();
                List<String> newList = new ArrayList<String>();
                oldList.forEach(newTempList::add);
                int i = 0;
                for(int e=0; e<newTempList.size(); e++)
                { 
                    if (newTempList.get(e).charAt(0) == '1')
                    {
                        q[0] = newTempList.get(e).split(",");
                        x1[0] = q[0][1];
                        x2[0] = q[0][2];
                        f[0].setLength(0);
                        f[0].append(x1[0]);
                        f[0].append(",");
                        f[0].append(x2[0]);
                        f[0].append(",k,k,k");
                        w[0] = f[0].toString();
                        newList.add(w[0]);
                    }
                    else if (newTempList.get(e).charAt(0) == '2')
                    {
                        i = 0;
                        q[0] = newTempList.get(e).split(",");
                        x1[0] = q[0][1];
                        x2[0] = q[0][2];
                        while(i<newList.size())
                        {
                            s[0] = newList.get(i).split(",");
                            y1[0] = s[0][0];
                            y2[0] = s[0][1];
                            y3[0] = s[0][2];
                            y4[0] = s[0][3];
                            y5[0] = s[0][4];
                            if(y2[0].equals(x1[0]))
                            {
                                f[0].setLength(0);
                                f[0].append(y1[0]);
                                f[0].append(",");
                                f[0].append(y2[0]);
                                f[0].append(",");
                                f[0].append(x2[0]);
                                f[0].append(",");
                                f[0].append(y4[0]);
                                f[0].append(",");
                                f[0].append(y5[0]);
                                w[0] = f[0].toString();
                                if(y3[0].equals("k"))
                                {
                                    newList.remove(i);
                                    newList.add(i,w[0]);
                                }
                                else
                                {
                                    newList.add(i+1, w[0]);
                                    i++;
                                }
                            }
                            i++;
                        }
                    }
                    else if (newTempList.get(e).charAt(0) == '3')
                    {
                        i = 0;
                        q[0] = newTempList.get(e).split(",");
                        x1[0] = q[0][1];
                        x2[0] = q[0][2];
                        while(i<newList.size())
                        {
                            s[0] = newList.get(i).split(",");
                            y1[0] = s[0][0];
                            y2[0] = s[0][1];
                            y3[0] = s[0][2];
                            y4[0] = s[0][3];
                            y5[0] = s[0][4];
                            if(y3[0].equals("k"))
                            {
                                newList.remove(i);
                                i--;
                            }
                            else if(y3[0].equals(x1[0]))
                            {
                                f[0].setLength(0);
                                f[0].append(y1[0]);
                                f[0].append(",");
                                f[0].append(y2[0]);
                                f[0].append(",");
                                f[0].append(y3[0]);
                                f[0].append(",");
                                f[0].append(x2[0]);
                                f[0].append(",");
                                f[0].append(y5[0]);
                                w[0] = f[0].toString();
                                if(y4[0].equals("k"))
                                {
                                    newList.remove(i);
                                    newList.add(i,w[0]);
                                }
                                else
                                {
                                    newList.add(i+1, w[0]);
                                    i++;
                                }
                            }
                            i++;
                        }
                    }
                    else if (newTempList.get(e).charAt(0) == '4')
                    {
                        
                        i = 0;
                        q[0] = newTempList.get(e).split(",");
                        x1[0] = q[0][1];
                        x2[0] = q[0][2];
                        while(i<newList.size())
                        {
                            s[0] = newList.get(i).split(",");
                            y1[0] = s[0][0];
                            y2[0] = s[0][1];
                            y3[0] = s[0][2];
                            y4[0] = s[0][3];
                            y5[0] = s[0][4];
                            if(y4[0].equals("k"))
                            {
                                newList.remove(i);
                                i--;
                            } 
                            else if(y4[0].equals(x1[0]))
                            { 
                                f[0].setLength(0);
                                f[0].append(y1[0]);
                                f[0].append(",");
                                f[0].append(y2[0]);
                                f[0].append(",");
                                f[0].append(y3[0]);
                                f[0].append(",");
                                f[0].append(y4[0]);
                                f[0].append(",");
                                f[0].append(x2[0]);
                                w[0] = f[0].toString();
                                if(y5[0].equals("k"))
                                {
                                    newList.remove(i);
                                    newList.add(i,w[0]);
                                }
                                else
                                {
                                    newList.add(i+1, w[0]);
                                    i++;
                                }
                            }
                            i++;
                        }
                    }
                }
                for(int r=0;r<newList.size();r++)
                {
                    if(newList.get(r).contains("k"))
                    {
                        newList.remove(r);
                        r--;
                    }
                }
                return newList;
            }
        };
        //sorting each reducer's dataset before reduce phase
        Function<Iterable<String>, Iterable<String>> sorting = new Function<Iterable<String>, Iterable<String>>()
        {
            @Override
            public Iterable<String> call(Iterable<String> oldList)
            {
                List<String> newList = new ArrayList<String>();
                oldList.forEach(newList::add);
                Collections.sort(newList);
                return newList;
            }
        };
        JavaRDD<Row> ws = d1.javaRDD(), ss = d2.javaRDD(), sr = d3.javaRDD(), cs = d4.javaRDD();
        JavaPairRDD<String, String> pair1 = ws.flatMapToPair(hashing1);
        JavaPairRDD<String, String> pair2 = ss.flatMapToPair(hashing2);
        JavaPairRDD<String, String> pair3 = sr.flatMapToPair(hashing3);
        JavaPairRDD<String, String> pair4 = cs.flatMapToPair(hashing4);
        JavaPairRDD<String, String> temp1BigPair = sc.union(pair1, pair2, pair3, pair4);
        JavaPairRDD<String, Iterable<String>> sorted = temp1BigPair.groupByKey().mapValues(sorting);
        JavaPairRDD<String, Iterable<String>> join = sorted.mapValues(reduce);
        List<Tuple2<String, Iterable<String>>> col = join.collect();
        for (Tuple2<String, Iterable<String>> t : col) {
            Iterable<String> list = t._2;
            System.out.println(t._1);
            for (String t2 : list) {
                System.out.println(t2);
            }
            System.out.println("=====");
        }
        Scanner reader = new Scanner(System.in);
        System.out.println("a1:" + a1 + " a2:" + a2 + " a3:" + a3);
        System.out.println("web_sales length:" + wsLen + " store_sales length:" + ssLen + " store_returns length:" + srLen + " catalog_sales length:" + csLen);
        System.out.println("Enter a number to stop: ");
        int n = reader.nextInt();
    }
}
