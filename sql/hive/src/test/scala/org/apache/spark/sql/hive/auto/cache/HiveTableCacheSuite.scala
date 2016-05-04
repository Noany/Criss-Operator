package org.apache.spark.sql.hive.auto.cache

//import org.apache.spark.sql.auto.cache.QGMaster
//import org.apache.spark.sql.auto.cache.QGMaster

import org.apache.spark.sql.auto.cache.QGMaster
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Logging, SparkContext, SparkConf}
import org.scalatest.FunSuite

/**
 * Created by zengdan on 15-4-27.
 */
class HiveTableCacheSuite extends FunSuite with Logging{

  System.setProperty("spark.sql.auto.cache", "true")
  System.setProperty("spark.sql.shuffle.partitions","2")

  System.setProperty("spark.tachyonStore.global.baseDir","/global_spark_tachyon")
  System.setProperty("spark.externalBlockStore.blockManager", "org.apache.spark.storage.ReuseBlockManager")
  System.setProperty("hive.metastore.warehouse.dir", "/Users/zengdan/hive")
  val conf = new SparkConf()
  conf.setAppName("TPCH").setMaster("local")
  //conf.set("spark.sql.shuffle.partitions","1")
  var sc = new SparkContext(conf)
  sc.hadoopConfiguration.set("fs.tachyon.impl","tachyon.hadoop.TFS")

  val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
  new Thread(){
    override def run(){
      QGMaster.main("--host localhost --port 7070".split(" "))
    }
  }.start()

  //val iter = 1
  val collect = false

  val res0 = sqlContext.sql("""select l_returnflag, l_linestatus, sum(l_quantity) , sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order
                                from lineitem
                                where l_shipdate <= '1998-09-16' and l_returnflag like '%R%'
                                group by l_returnflag, l_linestatus""")

  //println(res0.queryExecution.executedPlan(0))
  //order by l_returnflag, l_linestatus""")

  if(collect) res0.collect().foreach(println)
  else println("Result count is " + res0.count())

  test("TPCH Q1"){
    //QGMaster.main("--host localhost --port 7070".split(" "))

    //for (i <- 1 to 5) {
      for (query <- 1 to 22) {
        for (i <- 1 to 4) {
          logInfo(s"=======query $query=======")
          val q = query
          this.getClass.getMethod("executeQ" + q, Array.empty[Class[_]]: _*).invoke(this, Array.empty[Object]: _*)
          println(s"=======query $query=======")
        }
      }
    //}

  }

  //test TakeOrderedAndProject
  def executeQ0()={
    for(i <- 1 to iter) {

      val res0 = sqlContext.sql("""select l_returnflag, l_linestatus from lineitem
                                where l_shipdate <= '1998-09-16' and l_returnflag like '%R%'
                                 order by l_returnflag limit 1""")

      //res0.queryExecution.optimizedPlan(0).expressions.foreach(x => println(x.treeString))
      //order by l_returnflag, l_linestatus""")

      //println(res0.queryExecution.executedPlan(0))


      if(collect) res0.collect().foreach(println)
      else println("Result count is " + res0.count())
    }
  }



  //test exchange unmatch
  def executeQ24()={
    val res0 = sqlContext.sql("""select l_returnflag, l_linestatus, sum(l_quantity) , sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order
                                from lineitem
                                where l_shipdate <= '1998-09-16'
                                group by l_returnflag, l_linestatus""")
    res0.collect().foreach(println)
    sc.stop()

    val conf1 = new SparkConf()
    conf1.setAppName("TPCH2").setMaster("local")
    conf1.set("spark.sql.shuffle.partitions","2")
    val sc1 = new SparkContext(conf1)
    sc1.hadoopConfiguration.set("fs.tachyon.impl","tachyon.hadoop.TFS")

    val sqlContext1 = new org.apache.spark.sql.hive.HiveContext(sc1)

    val res1 = sqlContext1.sql("""select l_returnflag, l_linestatus, sum(l_quantity) , sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order
                                from lineitem
                                where l_shipdate <= '1998-09-16'
                                group by l_returnflag, l_linestatus""")
    res1.collect().foreach(println)
    sc.stop()

  }

  //test project subsumption
  def executeQ25()={
    val res0 = sqlContext.sql("""select l_returnflag, l_linestatus, l_shipmode, sum(l_quantity) , sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order
                                from lineitem
                                where l_shipdate <= '1998-09-16' and l_shipmode != 'mode'
                                group by l_returnflag, l_linestatus, l_shipmode""")

    res0.collect()

    val res = sqlContext.sql("""select l_returnflag, l_linestatus, l_shipmode, sum(l_quantity) , sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order
                                from lineitem
                                where l_shipdate <= '1998-09-16' and l_shipmode != 'mode'
                                group by l_returnflag, l_linestatus, l_shipmode""")
    res.collect()

    val res1 = sqlContext.sql("""select l_returnflag, l_linestatus, sum(l_quantity) , sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order
                                from lineitem
                                where l_shipdate <= '1998-09-16' and l_shipmode != 'mode'
                                group by l_returnflag, l_linestatus""")
    res1.collect()

  }

  //test different order filter expression match
  def executeQ23()={
    val res0 = sqlContext.sql("""select l_returnflag, l_linestatus, sum(l_quantity) , sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order
                                from lineitem
                                where l_shipdate <= '1998-09-16' or (l_quantity < 2 and l_extendedprice > 10 and l_extendedprice < 100)
                                group by l_returnflag, l_linestatus""").collect()
    //val x = res0.queryExecution.executedPlan
    //println(x(0))
    //res0.collect()

    val res1 = sqlContext.sql("""select l_returnflag, l_linestatus, sum(l_quantity) , sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order
                                from lineitem
                                where (l_extendedprice > 10 and l_quantity < 2 and 100 > l_extendedprice) or l_shipdate <= '1998-09-16'
                                group by l_returnflag, l_linestatus""").collect()
    //val y = res1.queryExecution.executedPlan
    //println(y(0))
    //res1.collect()

    val res2 = sqlContext.sql("""select l_returnflag, l_linestatus, sum(l_quantity) , sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order
                                from lineitem
                                where 2 > l_quantity  and l_shipdate <= '1998-09-16'
                                group by l_returnflag, l_linestatus""").collect()
    //val z = res1.queryExecution.executedPlan

    //println(z(0))
    //res2.collect()

    val res3 = sqlContext.sql("""select l_returnflag, l_linestatus, sum(l_quantity) , sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order
                                from lineitem
                                where '1998-09-16' >= l_shipdate and 2 > l_quantity
                                group by l_returnflag, l_linestatus""").collect()
    //val t = res1.queryExecution.executedPlan

    //println(t(0))
    //res3.collect()

  }

  def executeQ1()={
    for(i <- 1 to iter) {

      val res0 = sqlContext.sql("""select l_returnflag, l_linestatus, sum(l_quantity) , sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order
                                from lineitem
                                where l_shipdate <= '1998-09-16' and l_returnflag like '%R%'
                                group by l_returnflag, l_linestatus""")

      //println(res0.queryExecution.executedPlan(0))
      //order by l_returnflag, l_linestatus""")

      if(collect) res0.collect().foreach(println)
      else println("Result count is " + res0.count())
    }
  }

  def executeQ2()={
    for(i <- 1 to iter) {

      val res0 = sqlContext.sql("""select s.s_acctbal as s_acctbal, s.s_name as s_name, n.n_name as n_name, p.p_partkey as p_partkey, ps.ps_supplycost as ps_supplycost, p.p_mfgr as p_mfgr, s.s_address as s_address, s.s_phone as s_phone, s.s_comment as s_comment from nation n join region r on n.n_regionkey = r.r_regionkey and r.r_name = 'EUROPE' join supplier s on s.s_nationkey = n.n_nationkey join partsupp ps on s.s_suppkey = ps.ps_suppkey join part p on p.p_partkey = ps.ps_partkey and p.p_size = 15 and p.p_type like '%BRASS' """)
      ///*
      res0.persist(StorageLevel.MEMORY_AND_DISK)
      res0.registerTempTable("q2_minimum_cost_supplier_tmp1")


      val res1 = sqlContext.sql("""select p_partkey, min(ps_supplycost) as ps_min_supplycost from q2_minimum_cost_supplier_tmp1 group by p_partkey""")
      res1.registerTempTable("q2_minimum_cost_supplier_tmp2")

      val res2 = sqlContext.sql("""select t1.s_acctbal, t1.s_name, t1.n_name, t1.p_partkey, t1.p_mfgr, t1.s_address, t1.s_phone, t1.s_comment from q2_minimum_cost_supplier_tmp1 t1 join q2_minimum_cost_supplier_tmp2 t2 on t1.p_partkey = t2.p_partkey and t1.ps_supplycost=t2.ps_min_supplycost""") //order by s_acctbal desc, n_name, s_name, p_partkey limit 100

      if(collect) println("Result count is " + res2.collect().length)
      else println("Result count is " + res2.count())
    }


  }

  def executeQ3()= {
    for (i <- 1 to iter) {
      val res0 = sqlContext.sql("""select l_orderkey,sum(l_extendedprice * (1 - l_discount)) as revenue,o_orderdate,o_shippriority
                                from customer,orders,lineitem
                                where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < '1995-03-22' and l_shipdate > '1995-03-22'
                                group by l_orderkey,o_orderdate,o_shippriority""")
      //order by revenue desc,o_orderdate
      //limit 10""")
      if(collect) res0.collect()
      else println("Result count is " + res0.count())
    }
  }

  def executeQ4()= {
    for (i <- 1 to iter) {
      val res0 = sqlContext.sql("""
                                  select
                                DISTINCT l_orderkey as o_orderkey
                                  from
                                lineitem
                                where
                                l_commitdate < l_receiptdate""")
      res0.registerTempTable("q4_order_priority_tmp")
      //persist(StorageLevel.MEMORY_AND_DISK)

      val res1 = sqlContext.sql("""select o_orderpriority, count(1) as order_count
                                from orders o join q4_order_priority_tmp t
                                  on o.o_orderkey = t.o_orderkey and o.o_orderdate >= '1993-07-01' and o.o_orderdate < '1993-10-01'
                                group by o_orderpriority""")
      //order by o_orderpriority""")

      if(collect) res1.collect()
      else println("Result count is " + res1.count())
    }
  }

  def executeQ5()= {
    for (i <- 1 to iter) {
      val res0 = sqlContext.sql("""select n_name,sum(l_extendedprice * (1 - l_discount)) as revenue
                                from customer,orders,lineitem,supplier,nation,region
                                where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'AFRICA' and o_orderdate >= '1993-01-01' and o_orderdate < '1994-01-01'
                                group by n_name""")
      //order by revenue desc""")
      if(collect) res0.collect()
      else println("Result count is " + res0.count())
    }
  }

  def executeQ6()= {
    for (i <- 1 to iter) {
      val res0 = sqlContext.sql("""select sum(l_extendedprice * l_discount) as revenue
                                from lineitem where l_shipdate >= '1993-01-01' and l_shipdate < '1994-01-01' and l_discount between 0.06 - 0.01 and 0.06 + 0.01 and l_quantity < 25""")

      if(collect) res0.collect()
      else println("Result count is " + res0.count())
    }
  }

  def executeQ7()= {
    for (i <- 1 to iter) {
      val res0 = sqlContext.sql("""select supp_nation,cust_nation,l_year,sum(volume) as revenue
                                from (select n1.n_name as supp_nation,n2.n_name as cust_nation,year(l_shipdate) as l_year,l_extendedprice * (1 - l_discount) as volume
                                from supplier,lineitem,orders,customer,nation n1,nation n2
                                where s_suppkey = l_suppkey and o_orderkey = l_orderkey and c_custkey = o_custkey and s_nationkey = n1.n_nationkey and c_nationkey = n2.n_nationkey and ((n1.n_name = 'KENYA' and n2.n_name = 'PERU')or (n1.n_name = 'PERU' and n2.n_name = 'KENYA')) and l_shipdate between '1995-01-01' and '1996-12-31') as shipping
                                group by supp_nation,cust_nation,l_year""")
      //order by supp_nation,cust_nation,l_year"""

      if(collect) {


        res0.collect()
        //println("=======print results=======")
        //ret.foreach(println)
        //println("=======print results=======")
      }
      else println("Result count is " + res0.count())
    }
  }

  def executeQ8()= {
    for (i <- 1 to iter) {
      val res0 = sqlContext.sql("""select o_year, sum(case when nation = 'BRAZIL' then volume else 0.0 end) / sum(volume) as mkt_share from ( select year(o_orderdate) as o_year, l_extendedprice * (1-l_discount) as volume, n2.n_name as nation from nation n2 join (select o_orderdate, l_discount, l_extendedprice, s_nationkey from supplier s join (select o_orderdate, l_discount, l_extendedprice, l_suppkey from part p join (select o_orderdate, l_partkey, l_discount, l_extendedprice, l_suppkey from lineitem l join (select o_orderdate, o_orderkey from orders o join (select c.c_custkey from customer c join (select n1.n_nationkey from nation n1 join region r on n1.n_regionkey = r.r_regionkey and r.r_name = 'AMERICA' ) n11 on c.c_nationkey = n11.n_nationkey ) c1 on c1.c_custkey = o.o_custkey ) o1 on l.l_orderkey = o1.o_orderkey and o1.o_orderdate >= '1995-01-01' and o1.o_orderdate < '1996-12-31' ) l1 on p.p_partkey = l1.l_partkey and p.p_type = 'ECONOMY ANODIZED STEEL' ) p1 on s.s_suppkey = p1.l_suppkey ) s1 on s1.s_nationkey = n2.n_nationkey ) all_nation group by o_year""")// order by o_year""")
      if(collect) {
        val ret = res0.collect()
        println("=======print results=======")
        ret.foreach(println)
        println("=======print results=======")
      }
      else println("Result count is " + res0.count())
    }
  }

  def executeQ9()= {
    for (i <- 1 to iter) {
      val res0 = sqlContext.sql(
        """select nation, o_year, sum(amount) as sum_profit from ( select n_name as nation, year(o_orderdate) as o_year, l_extendedprice * (1 - l_discount) -  ps_supplycost * l_quantity as amount from orders o join (select l_extendedprice, l_discount, l_quantity, l_orderkey, n_name, ps_supplycost from part p join (select l_extendedprice, l_discount, l_quantity, l_partkey, l_orderkey, n_name, ps_supplycost from partsupp ps join (select l_suppkey, l_extendedprice, l_discount, l_quantity, l_partkey, l_orderkey, n_name from (select s_suppkey, n_name from nation n join supplier s on n.n_nationkey = s.s_nationkey ) s1 join lineitem l on s1.s_suppkey = l.l_suppkey ) l1 on ps.ps_suppkey = l1.l_suppkey and ps.ps_partkey = l1.l_partkey ) l2 on p.p_name like '%green%' and p.p_partkey = l2.l_partkey ) l3 on o.o_orderkey = l3.l_orderkey )profit group by nation, o_year""")// order by nation, o_year desc""")

      if(collect) res0.collect()
      else println("Result count is " + res0.count())
    }
  }

  def executeQ10()= {
    for (i <- 1 to iter) {
      val res0 = sqlContext.sql("""select c_custkey,c_name,sum(l_extendedprice * (1 - l_discount)) as revenue,c_acctbal,n_name,c_address,c_phone,c_comment
                                from customer,orders,lineitem,nation
                                where c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate >= '1993-07-01' and o_orderdate < '1993-10-01' and l_returnflag = 'R' and c_nationkey = n_nationkey
                                group by c_custkey,c_name,c_acctbal,c_phone,n_name,c_address,c_comment""")
      //order by revenue desc
      //limit 20""")
      val res1 = sqlContext.sql("""select c_custkey,c_name,l_extendedprice * (1 - l_discount) as revenue,c_acctbal,n_name,c_address,c_phone,c_comment
                                from customer,orders,lineitem,nation
                                where c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate >= '1993-07-01' and o_orderdate < '1993-10-01' and l_returnflag = 'R' and c_nationkey = n_nationkey
                                """)

      if(collect) res0.collect()
      else println("Result count is " + res0.count())
    }
  }

  def executeQ11()= {
    for (i <- 1 to iter) {
      val res0 = sqlContext.sql("""select ps_partkey, sum(ps_supplycost * ps_availqty) as part_value from nation n join supplier s on s.s_nationkey = n.n_nationkey and n.n_name = 'GERMANY' join partsupp ps on ps.ps_suppkey = s.s_suppkey group by ps_partkey""")
      res0.registerTempTable("q11_part_tmp")
      //persist(StorageLevel.MEMORY_AND_DISK).registerTempTable("q11_part_tmp")

      val res1 = sqlContext.sql("""select sum(part_value) as total_value from q11_part_tmp""")
      res1.registerTempTable("q11_sum_tmp")

      val res2 = sqlContext.sql("""select ps_partkey, part_value as value from ( select ps_partkey, part_value, total_value from q11_part_tmp join q11_sum_tmp ) a where part_value > total_value * 0.0001""") // order by value desc""")

      if(collect) println("Result count is " + res2.collect().length)
      else println("Result count is " + res2.count())
    }
  }

  def executeQ12()= {
    for (i <- 1 to iter) {
      val res0 = sqlContext.sql("""select l_shipmode,sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1 else 0 end) as high_line_count,sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count
                                from orders,lineitem
                                where o_orderkey = l_orderkey and l_shipmode in ('REG AIR', 'MAIL') and l_commitdate < l_receiptdate and l_shipdate < l_commitdate and l_receiptdate >= '1995-01-01' and l_receiptdate < '1996-01-01'
                                group by l_shipmode""")
      //order by l_shipmode""")

      if(collect) {
        println("======print result=====")
        res0.collect().foreach(println)
        println("======print result=====")
      }
      else println("Result count is " + res0.count())
    }
  }

  def executeQ13()= {
    for (i <- 1 to iter) {
      val res0 = sqlContext.sql("""select c_count,count(*) as custdist
                                from (select c_custkey,count(o_orderkey) as c_count
                                from customer left outer join orders on c_custkey = o_custkey and o_comment not like '%special%requests%'
                                group by c_custkey) c_orders
                                group by c_count""")
      //order by custdist desc,c_count desc""")

      if(collect) res0.collect()
      else println("Result count is " + res0.count())
    }
  }

  def executeQ14()= {
    for (i <- 1 to iter) {
      val res0 = sqlContext.sql("""select 100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount) else 0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
                                from lineitem,part
                                where l_partkey = p_partkey and l_shipdate >= '1995-08-01'and l_shipdate < '1995-09-01'""")

      if(collect) res0.collect()
      else println("Result count is " + res0.count())
    }
  }

  def executeQ15()= {
    for (i <- 1 to iter) {
      val res2 = sqlContext.sql("""
                                  select l_suppkey as supplier_no,sum(l_extendedprice * (1 - l_discount)) as total_revenue
                                from lineitem
                                  where l_shipdate >= '1996-01-01' and l_shipdate < '1996-04-01'
                                group by l_suppkey""")
      res2.persist(StorageLevel.MEMORY_AND_DISK).registerTempTable("revenue_cached")

      val res3 = sqlContext.sql("""
                                  select max(total_revenue) as max_revenue
                                from revenue_cached""")
      res3.registerTempTable("max_revenue_cached")

      val res4 = sqlContext.sql("""select s_suppkey,s_name,s_address,s_phone,total_revenue
                                from supplier,revenue_cached,max_revenue_cached
                                where s_suppkey = supplier_no and total_revenue = max_revenue""")
      //order by s_suppkey""")

      if(collect) res4.collect()
      else println("Result count is " + res4.count())
    }
  }

  def executeQ16()= {
    for (i <- 1 to iter) {
      val res2 = sqlContext.sql("""
                                select
                                  s_suppkey
                                from
                                  supplier
                                where
                                  not s_comment like '%Customer%Complaints%'""")
      res2.registerTempTable("supplier_tmp")

      val res3 = sqlContext.sql("""
                                select
                                  p_brand, p_type, p_size, ps_suppkey
                                from
                                  partsupp ps join part p
                                  on
                                    p.p_partkey = ps.ps_partkey and p.p_brand <> 'Brand#45'
                                    and not p.p_type like 'MEDIUM POLISHED%'
                                  join supplier_tmp s
                                  on
                                    ps.ps_suppkey = s.s_suppkey""")
      res3.registerTempTable("q16_tmp")

      val res4 = sqlContext.sql("""select
                                  p_brand, p_type, p_size, count(distinct ps_suppkey) as supplier_cnt
                                from
                                  (select
                                     *
                                   from
                                     q16_tmp
                                   where p_size = 49 or p_size = 14 or p_size = 23 or
                                         p_size = 45 or p_size = 19 or p_size = 3 or
                                         p_size = 36 or p_size = 9
                                ) q16_all
                                group by p_brand, p_type, p_size""")
      //order by supplier_cnt desc, p_brand, p_type, p_size""")

      if(collect) res4.collect()
      else println("Result count is " + res4.count())
    }
  }

  def executeQ17()= {
    for (i <- 1 to iter) {
      val res1 = sqlContext.sql("""
                                  select l_partkey as t_partkey,0.2 * avg(l_quantity) as t_avg_quantity
                                from lineitem
                                  group by l_partkey""")
      res1.registerTempTable("q17_lineitem_tmp_cached")

      val res2 = sqlContext.sql("""select sum(l_extendedprice) / 7.0 as avg_yearly
                                from (select l_quantity,l_extendedprice,t_avg_quantity from q17_lineitem_tmp_cached join (select l_quantity,l_partkey,l_extendedprice from part,lineitem where p_partkey = l_partkey and p_brand = 'Brand#23' and p_container = 'MED BOX') l1 on l1.l_partkey = t_partkey) a
                                where l_quantity < t_avg_quantity""")

      if(collect) res2.collect()
      else println("Result count is " + res2.count())
    }
  }

  def executeQ18()= {
    for (i <- 1 to iter) {
      val res1 = sqlContext.sql("""
                                select l_orderkey,sum(l_quantity) as t_sum_quantity
                                from lineitem
                                where l_orderkey is not null
                                group by l_orderkey""")
      res1.registerTempTable("q18_tmp_cached")

      val res2 = sqlContext.sql("""select c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum(l_quantity)
                                from customer,orders,q18_tmp_cached t,lineitem l
                                where c_custkey = o_custkey and o_orderkey = t.l_orderkey and
                                o_orderkey is not null and t.t_sum_quantity > 300 and o_orderkey = l.l_orderkey
                                and l.l_orderkey is not null
                                group by c_name, c_custkey,o_orderkey,o_orderdate,o_totalprice""")
      //order by o_totalprice desc, o_orderdate""")

      if(collect) res2.collect()
      else println("Result count is " + res2.count())
    }
  }

  def executeQ19()= {
    for (i <- 1 to iter) {
      val res0 = sqlContext.sql("""select sum(l_extendedprice * (1 - l_discount) ) as revenue
                                from lineitem l join part p
                                  on p.p_partkey = l.l_partkey
                                where (p_brand = 'Brand#12' and p_container REGEXP 'SM CASE||SM BOX||SM PACK||SM PKG' and l_quantity >= 1 and l_quantity <= 11 and p_size >= 1 and p_size <= 5 and l_shipmode REGEXP 'AIR||AIR REG' and l_shipinstruct = 'DELIVER IN PERSON') or (p_brand = 'Brand#23' and p_container REGEXP 'MED BAG||MED BOX||MED PKG||MED PACK' and l_quantity >= 10 and l_quantity <= 20 and p_size >= 1 and p_size <= 10 and l_shipmode REGEXP 'AIR||AIR REG' and l_shipinstruct = 'DELIVER IN PERSON') or (p_brand = 'Brand#34' and p_container REGEXP 'LG CASE||LG BOX||LG PACK||LG PKG' and l_quantity >= 20 and l_quantity <= 30 and p_size >= 1 and p_size <= 15 and l_shipmode REGEXP 'AIR||AIR REG' and l_shipinstruct = 'DELIVER IN PERSON')""")

      if(collect) res0.collect()
      else println("Result count is " + res0.count())
    }
  }

  def executeQ20()= {
    for (i <- 1 to iter) {
      val res4 = sqlContext.sql("""
                                  select distinct p_partkey as p_partkey
                                from part
                                  where p_name like 'forest%'""")
      res4.registerTempTable("q20_tmp1_cached")

      val res5 = sqlContext.sql("""
                                  select l_partkey,l_suppkey,0.5 * sum(l_quantity) as sum_quantity
                                from lineitem
                                  where l_shipdate >= '1994-01-01' and l_shipdate < '1995-01-01'
                                group by l_partkey, l_suppkey""")
      res5.registerTempTable("q20_tmp2_cached")

      val res6 = sqlContext.sql("""
                                  select ps_suppkey, ps_availqty, sum_quantity
                                from partsupp, q20_tmp1_cached, q20_tmp2_cached
                                where ps_partkey = p_partkey and ps_partkey = l_partkey and ps_suppkey = l_suppkey""")
      res6.registerTempTable("q20_tmp3_cached")

      val res7 = sqlContext.sql("""
                                  select ps_suppkey
                                  from q20_tmp3_cached
                                  where ps_availqty > sum_quantity
                                  group by ps_suppkey""")
      res7.registerTempTable("q20_tmp4_cached")

      val res8 = sqlContext.sql("""select s_name,s_address
                                from supplier,nation,q20_tmp4_cached
                                where s_nationkey = n_nationkey and n_name = 'CANADA' and s_suppkey = ps_suppkey""")
      //order by s_name""")

      if(collect) res8.collect()
      else println("Result count is " + res8.count())
    }
  }

  def executeQ21()= {
    for (i <- 1 to iter) {
      val res2 = sqlContext.sql("""
                                select l_orderkey,count(distinct l_suppkey) as count_suppkey,max(l_suppkey) as max_suppkey
                                from lineitem
                                where l_orderkey is not null
                                group by l_orderkey""")
      res2.registerTempTable("q21_tmp1_cached")

      val res3 = sqlContext.sql("""
                                  select l_orderkey,count(distinct l_suppkey) as count_suppkey,max(l_suppkey) as max_suppkey
                                from lineitem
                                  where l_receiptdate > l_commitdate and l_orderkey is not null
                                group by l_orderkey""")
      res3.registerTempTable("q21_tmp2_cached")

      val res4 = sqlContext.sql("""select s_name,count(1) as numwait
                                from (select s_name from (select s_name,t2.l_orderkey,l_suppkey,count_suppkey,max_suppkey from q21_tmp2_cached t2 right outer join (select s_name,l_orderkey,l_suppkey from (select s_name,t1.l_orderkey,l_suppkey,count_suppkey,max_suppkey from q21_tmp1_cached t1 join (select s_name,l_orderkey,l_suppkey from orders o join (select s_name,l_orderkey,l_suppkey from nation n join supplier s on s.s_nationkey = n.n_nationkey and n.n_name = 'SAUDI ARABIA' join lineitem l on s.s_suppkey = l.l_suppkey where l.l_receiptdate > l.l_commitdate and l.l_orderkey is not null) l1 on o.o_orderkey = l1.l_orderkey and o.o_orderstatus = 'F') l2 on l2.l_orderkey = t1.l_orderkey) a where (count_suppkey > 1) or ((count_suppkey=1) and (l_suppkey <> max_suppkey))) l3 on l3.l_orderkey = t2.l_orderkey) b where (count_suppkey is null) or ((count_suppkey=1) and (l_suppkey = max_suppkey))) c group by s_name""")
      //order by numwait desc,s_name
      //limit 100""")

      if(collect) println("Result count is " + res4.collect().length)
      else println("Result count is " + res4.count())
    }
  }

  def executeQ22()= {
    for (i <- 1 to iter) {
      val res3 = sqlContext.sql("""
                                select c_acctbal,c_custkey,substr(c_phone, 1, 2) as cntrycode
                                from customer
                                where substr(c_phone, 1, 2) = '13' or substr(c_phone, 1, 2) = '31' or substr(c_phone, 1, 2) = '23' or substr(c_phone, 1, 2) = '29' or substr(c_phone, 1, 2) = '30' or substr(c_phone, 1, 2) = '18' or substr(c_phone, 1, 2) = '17'""")
      res3.registerTempTable("q22_customer_tmp_cached")
        //persist(StorageLevel.MEMORY_AND_DISK).registerTempTable("q22_customer_tmp_cached")

      val res4 = sqlContext.sql("""
                                select avg(c_acctbal) as avg_acctbal
                                from q22_customer_tmp_cached
                                where c_acctbal > 0.00""")
      res4.registerTempTable("q22_customer_tmp1_cached")

      val res5 = sqlContext.sql("""
                                select o_custkey
                                from orders
                                group by o_custkey""")
      res5.registerTempTable("q22_orders_tmp_cached")

      val res6 = sqlContext.sql("""select cntrycode,count(1) as numcust,sum(c_acctbal) as totacctbal
                                from (select cntrycode,c_acctbal,avg_acctbal from q22_customer_tmp1_cached ct1 join (select cntrycode,c_acctbal from q22_orders_tmp_cached ot right outer join q22_customer_tmp_cached ct on ct.c_custkey = ot.o_custkey where o_custkey is null) ct2) a where c_acctbal > avg_acctbal
                                group by cntrycode""")
      //order by cntrycode""")

      if(collect) res6.collect()
      else println("Result count is " + res6.count())
    }
  }


  test("Create Hive Table"){
    //System.setProperty("spark.sql.shuffle.partitions","2")
    //System.setProperty("hive.metastore.warehouse.dir", "/Users/zengdan/hive")
    //val conf = new SparkConf()
    //conf.setAppName("Create Table").setMaster("local")
    //val sc = new SparkContext(conf)
    //sc.hadoopConfiguration.set("fs.tachyon.impl","tachyon.hadoop.TFS")

    //val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val path = "hdfs://localhost:9000/user/zengdan/tpch"

    val lineitem = sqlContext.sql("create external table if not exists  lineitem (l_orderkey int, l_partkey int, l_suppkey int, " +
      "l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string," +
      " l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, " +
      "l_shipmode string, l_comment string) row format delimited fields terminated by '|' stored as textfile location '"
      +path+"/lineitem'")
    lineitem.count()
    println("table lineitem is successfully created!")

    val part = sqlContext.sql("create external table if not exists part (p_partkey int, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double, p_comment string) row format delimited fields terminated by '|' stored as textfile location '"+path+"/part'")
    part.count()
    println("table part is successfully created!")

    val supplier = sqlContext.sql("create external table if not exists supplier (s_suppkey int, s_name string, s_address string, s_nationkey int, s_phone string, s_acctbal double, s_comment string) row format delimited fields terminated by '|' stored as textfile location '" + path + "/supplier'")
    supplier.count()
    println("table supplier is successfully created!")

    val partsupp = sqlContext.sql("create external table if not exists partsupp (ps_partkey int, ps_suppkey int, ps_availqty int, ps_supplycost double, ps_comment string) row format delimited fields terminated by '|' stored as  textfile location '" + path + "/partsupp'")
    partsupp.count()
    println("table partsupp is successfully created!")

    val nation = sqlContext.sql("create external table if not exists nation (n_nationkey int, n_name string, n_regionkey int, n_comment string) row format delimited fields terminated by '|' stored as textfile location '" + path + "/nation'")
    nation.count()
    println("table nation is successfully created!")

    val region = sqlContext.sql("create external table if not exists region (r_regionkey int, r_name string, r_comment string) row format delimited fields terminated by '|' stored as textfile location '" + path + "/region'")
    region.count()
    println("table region is successfully created!")

    val orders = sqlContext.sql("create external table if not exists orders (o_orderkey int, o_custkey int, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int, o_comment string) row format delimited fields terminated by '|' stored as textfile location '" + path + "/orders'")
    orders.count()
    println("table orders is successfully created!")

    val customer = sqlContext.sql("create external table if not exists customer (c_custkey int, c_name string, c_address string, c_nationkey int, c_phone string, c_acctbal double, c_mktsegment string, c_comment string) row format delimited fields terminated by '|' stored as textfile location '" + path + "/customer'")
    customer.count()
    println("table customer is successfully created!")
  }

}
