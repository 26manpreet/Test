https://spark.apache.org/docs/latest/sql-programming-guide.html
http://spark.apache.org/docs/2.1.0/api/python/pyspark.sql.html?highlight=withcolumn#pyspark.sql.HiveContext

#====================================================================
# SPARK SQL
#====================================================================
- Spark SQL is spark library that can process for both structured ( Relational database) and unstructured data (JSON)
- spark SQL provides lot of API's for data operations
- fundamental data structure is DataFrames (RDD with structure).
- Dataframes in PySpark support both sql queries (select * from ) and expression (df.select())

    Dataset     : collection of data
    DataFrame   : collection of data with column names , similar to tables


- As sparkcontext is entry point for RDD. similarly,SparkSession provides a single entry point for dataframes.
- SparkSession is used to create Dataframes,register dataframes and execute sql queries.

           

Advantages:
    - SQL people can easliy work with it
    - can be used to read data from HIVE
    - can create user-defined functions(UDF) to increase spark functionalities
        1. create UDF
        2. register it in list of functions



#====================================================================
# Intialising Spark Session
#====================================================================
- SparkSession is available as spark in PySpark.  
- pyspark gives below objects by default
    - sc
    - spark
    
# or , create spark object explicitly
    from pyspark.sql import SparkSession  # pyspark.sql --> sql is subpackage of pyspark , SparkSession is class present in session module of pyspark.sql ( imports with pyspark.sql -> __init__.py)
    spark = SparkSession.builder.master('local').appName("Pyspark_example").config("spark.some.config.option", "some-value").getOrCreate()
                                # or, master('local[2]') -- run in standalone mode , integer represent number of partitions created by RDD
    
                    master           :   Sets the Spark master URL to connect to, such as "local" to run locally, "local[4]"
                                         to run locally with 4 cores, or "spark://master:7077" to run on a Spark standalone cluster.
                            
                    appName         :   Sets a name for the application, which will be shown in the Spark web UI.
                                        If no application name is set, a randomly generated name will be used.
                            
                    getorCreate()   :   This returns a SparkSession object if already exists, creates new one if not exists.            



# stop spark session
    spark.stop()

#====================================================================
# Create DataFrames
#====================================================================
- Structured data files
- RDD
- HIVE tables
- External tables


#1. from existing RDD's
    RDD = sc.parallelize([ ('XS',2018,55000),('XR',2019,42000)])
    names = ['Model','Year','Price']
    iphone_df = spark.createDataFrame(RDD,schema=names)
    #- schema -> column name , type of data in column and empty values.
    type(iphone_df)



#2. from (csv,json,txt)
    
    # header -> use first line as column , by default column names are _C01,_C02,......
    # schema with StructType is used for spark.read
    # inferSchema -> decide datatype implicity , by reading entire data . Hence, for large dataset performance may impact.
    df_csv  = spark.read.csv('orders.txt', header=True, inferSchema=True,sep=":")      # schema = , can define own schema too with StructType 
                                                                                       # mode = PERMISSIVE    :  
                                                                                                DROPMALFORMED : DROPMALFORMED : ignores the whole corrupted records.
                                                                                                FAILFAST      :   throws an exception when it meets corrupted records.
    # Read multple files
    df_csv  = spark.read.csv(['orders_1.txt','orders_2.txt','orders_3.txt'], header=True, inferSchema=True,sep=":") 

    df_json = spark.read.json('orders.txt', header=True, inferSchema=True)
    df_txt  = spark.read.txt('orders.txt', header=True, inferSchema=True)
    df_orc  = spark.read.orc("examples/src/main/resources/users.orc")




    df = spark.read.load("examples/src/main/resources/people.json", format="json")
    df = spark.read.load("examples/src/main/resources/people.csv",format="csv", sep=":", inferSchema="true", header="true")
    #df_csv = spark.read.format('csv').option('sep',',').schema('order_id','order_date',......).load(path)
    
        
    #--------INSPECT DATA----------------
    df_csv.columns      # List of column names       --> ['order_id', 'order_dt', 'product_id', 'order_status']
    df_csv.dtypes       # List of tuples (column_name,type) --> [('order_id', 'int'), ('order_dt', 'timestamp'), ('product_id', 'int'), ('order_status', 'string')]
    df_csv.printSchema()# column name, type and nullable
                             #root
                           # - first_name: string (nullable = true)
                           # - middle_name: string (nullable = true)
                           # - last_name: string (nullable = true)
                           # - dob: string (nullable = true)
                           # - gender: string (nullable = true)
                           # - salary: long (nullable = true)
    df_csv.first()  # Row(order_id=1, order_dt=datetime.datetime(2013, 7, 25, 0, 0), product_id=11599, order_status='CLOSED'
                    # row can be accessed as :
                    #    row = Row(order_id=1)
                    #    Row.order_id or Row['order_id']
    df_csv.head(2)  # return first n rows in list of rows --> [Row(order_id=1, order_dt=datetime.datetime(2013, 7, 25, 0, 0), product_id=11599, order_status='CLOSED'), Row(order_id=2,
    df_csv.describe()    #  DataFrame[summary: string, order_id: string, product_id: string, order_status: string]
                         #  +-------+------------------+------------------+------------+
                         #  |summary|          order_id|        product_id|order_status|
                         #  +-------+------------------+------------------+------------+
                         #  |  count|                10|                10|          10|
                         #  |   mean|               5.5|            6998.7|        null|
                         #  | stddev|3.0276503540974917|3961.0179401540486|        null|
                         #  |    min|                 1|               256|      CLOSED|
                         #  |    max|                10|             12111|  PROCESSING|
                         #  +-------+------------------+------------------+------------+
    df_csv.count()
    
    df_csv.show()               # default top 20 rows
    df_csv.show(500)            # top 500 rows
    df_csv.show(truncate=False) # Show full content of column in dataframe. By default, long columns are truncated
    df_csv.disinct().show()     # show distinct values
    
    
    df_csv.collect()            # return data as list of Row.
        #>>> df.collect()
        #[Row(age=2, name=u'Alice'), Row(age=5, name=u'Bob')]
    
    df.limit(2).show()         # return first 2 rows
    



#3. from HIVE 
    - for this spark and hive should be integerated
    orders = spark.read.table('retail.order')
    spark.sql('select * from retail.orders').show()
    
    

#4. From External database ( check for orcle too)
    - reading data from JDBC (e.g from mysql via jdbc)




#-----------------------------Simple Example-----------------------------------------
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

data = [("James","","Smith","36636","M",60000),
        ("Michael","Rose","","40288","M",70000),
        ("Robert","","Williams","42114","",400000),
        ("Maria","Anne","Jones","39192","F",500000),
        ("Jen","Mary","Brown","","F",0)]

columns = ["first_name","middle_name","last_name","dob","gender","salary"]
df = spark.createDataFrame(data = data, schema = columns) 

# columns can be defined in list for createDataframe but doesnt work for spark.read - StructType
df.printSchema()
#pysparkDF.show(truncate=False)

#root
# |-- first_name: string (nullable = true)
# |-- middle_name: string (nullable = true)
# |-- last_name: string (nullable = true)
# |-- dob: string (nullable = true)
# |-- gender: string (nullable = true)
# |-- salary: long (nullable = true)


# illustration of user defined schema in spark.read
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType
schema = StructType([ \
    StructField("order_id",IntegerType(),True), \
    StructField("order_dt",DateType(),True), \
    StructField("product_id",IntegerType(),True), \
    StructField("order_status", StringType(), True), \
  ])
df_csv = spark.read.csv('C:\\Users\Money\Desktop\orders.txt',sep=",",schema=schema)
#-------------------------------------------------------------------------------------


# RDD to DF
    df = RDD.toDF()
    df = spark.createDataFrame(RDD, schema = )

# DF to RDD
    df.rdd
    #type(df.rdd)

# Dataframe to Pandas
    pandasDF = DF.toPandas()

# datafreme to JSON
    JSON_DF = DF.toJSON()





# WRITE INTO CSV FILE



#====================================================================
# DATAFRAME OPERATIONS
#====================================================================
 - select
 - filter
 - join
 - aggragte
 - sort
 - analytical functions
 
#--------------------------------------------
#------------------SELECT-------------------- 
#--------------------------------------------

    help(df.select)
    
    
# SIMPLE WAYS TO SELECT COLUMNS    
    #select single column
    df.select('order_id').show()  # df_order.select('order_id') --. will return a dataframe with single column to print it use show()
    
    #select multiple columns
    df.select('order_id','order_date')  # also df_order.select(orders.order_id,orders.order_date)
    
    #select all columns
    df.select('*').show()
    
    # get complete date
    df.select('order_id','order_date').show(truncate=False)


# OTHER ALTERNATIVE WAYS TO SELECT COLUMNS  
    #Using Dataframe object name
    df.select(df_order.order_id,df_order.order_dt).show()   # This way is used in expressions
    
    # Using col function
    from pyspark.sql.functions import col
    df.select(col("order_id"),col("order_dt")).show()     
 
 
# ALIAS OF COLUMN    
    df.select(substring('order_date',1,3).alias('order_month')).show()    # check for mltiple columns
    #df_order.select('order_id',substring('order_id',1,3).alias('order_month')).show()    # check for mltiple columns
    
    
# FUNCTIONS    
    df.select(substring('order_date',1,3),'order_date')

# CASE    
    df.select(df.name, F.when(df.age > 4, 1).when(df.age < 3, -1).otherwise(0)).show()

# LIKE
    df.select("first_name",df.last_name.like("Singh")).show()

# STARTSWITH -ENDSWITH   
    df.title.startswith("THE")).show(5) # similar to like
    df.title.endswith("NT")).show(5)
    


# ADD COLUMN
    df.withColumn('order_month',substring('order_id',1,3)).show()  # withColumn ( new_Coulmn , new_column_expr) , new_column will replace exitsing column if with same name exists

# UPDATE COLUMN with some value
    df.withColumn("Price",col("Price")*100))   # since price column already exits, new column will replace it with its expression
    
# CHANGE COLUMN TYPE 
    df_new = df.withColumn("Price",col("Price").cast("Integer"))   # check if --> df.select(df.order_id.cast('int'))  , cast -> doesn't works with string column (' column_name')
    
    
#RENAME COLUMN NAME
    df.withColumnRenamed('col1','column1')


#DROP COLUMN
    df_new = df.drop('order_status')
    
    # or, fetch all column except 1 or more fileds 
    df.drop('order_status','order_id').show() # wont affect original dataframe, as df_order is not assigned further
    

    
# ACCESS COLUMNS WITH RAW SQL    
    # 1. create local temporary view
    df.createTempView('orders_VW')
    # 2. check tables
    spark.sql('show tables')
    # 3. access table with SQL
    spark.sql('select * from orders_VW').show()

    
    # create or replace temp view
    df.createOrReplaceTempView("orders_VW")    # registerTempTable , same as local temp view


    # create global temp view - global_temp
    df.createGlobalTempView("orders_VW")
    df2 = spark.sql("select * from global_temp.orders_VW")

    
    
    
    
# Converting dataframe into an RDD
    rdd = df.rdd
    # Converting dataframe into a RDD of string dataframe.toJSON().first()
    # Obtaining contents of df as Pandas 
    df.toPandas()
    
# Write & Save File in .parquet format
    dataframe.select("author", "title", "rank", "description") \
    .write \
    .save("Rankings_Descriptions.parquet")
 
 



#-----------------------------------------------
#-----------------  DATES ----------------------
#-----------------------------------------------
df = spark.createDataFrame( data= [('2020-02-20','2021-10-18',)], schema = ['start_dt','end_dt'])
     # type would be String
df.show()
    """
    +----------+----------+
    |  start_dt|    end_dt|
    +----------+----------+
    |2020-02-20|2021-10-18|
    +----------+----------+
    """
# current date and time 
    from pyspark.sql.functions import current_date,current_timestamp
    df.withColumn('curr_dt',current_date()).show()
    """
    +----------+----------+----------+
    |  start_dt|    end_dt|   curr_dt|
    +----------+----------+----------+
    |2020-02-20|2021-10-18|2021-03-11|
    +----------+----------+----------+
    """

    df.withColumn('curr_time',current_timestamp()).show(truncate=False)
    """
    +----------+----------+-----------------------+
    |start_dt  |end_dt    |curr_time              |
    +----------+----------+-----------------------+
    |2020-02-20|2021-10-18|2021-03-11 18:51:13.174|
    +----------+----------+-----------------------+
    """


# date to string
    - date_format() - converts date/string to string value

    from pyspark.sql.functions import date_format
    df.select('start_Dt',date_format("start_dt",'dd/MM/yyyy').alias("dt_format")).show()
        """
        +----------+----------+
        |  start_Dt| dt_format|
        +----------+----------+
        |2020-02-20|20/02/2020|
        +----------+----------+
        """
    df.select('start_Dt',date_format("start_dt",'YYYY').alias("year")).show()
        """
        +----------+----+
        |  start_Dt|year|
        +----------+----+
        |2020-02-20|2020|
        +----------+----+
        """

# string to date
    - to_date() - converts string column only to date

    from pyspark.sql.functions import to_date
    df.select(to_date('start_dt').alias("dt_format")).show()
        """
        +----------+
        | dt_format|
        +----------+
        |2020-02-20|
        +----------+
        """
# There are lot of other functions also available   --> pyspark.sql.functions  
    date_add("start_dt",2)
    add_months("start_dt",2)
    quarter("start_dt")
    last_day("start_dt")
    datediff("end_dt","start_dt")
    next_day("start_dt","Sun")
    months_between("end_dt","start_dt")


# ---------------------------------------------
#-------------- REPLACE --------------------
# ---------------------------------------------
    df.replace('James','XXXX').show() # in complte dataset

    df.replace('M','Male',subset= ['gender']).show()  # for gender column only

 #--------------------------------------------
 #-------------------FILTER-------------------
 #--------------------------------------------
 - Both filter() or where() works exaclty same , you can use where() if you have come from sql background
 - where is an alias for filter
 
    df.filter(df_order.order_status=='COMPLETE').show()
    # or, df_order.filter('order_Status'=='COMPLETE').show()
    
    df.where(df_order.order_status=='COMPLETE').show()
 
    
    # orders which are complte and were placed in August 2013 , IN and LIKE
    df.filter( ( df.order_Status.isin ('COMPLETE','CLOSED') ) &  ( df.order_date.like('2013-08%')) ).show()   # and -> & , or -> | , not -> ~
                    
    #orders that were placed on first day of month
    df.filter(date_format(df.order_date,'dd')=='01').select('order_date').show()()
                    
    df.select('order_id','order_date').where(df.order_status.isin('OPEN')).show()
    
    # between
    df.select(df.order_id.between(22, 24)).show() 



#--------------------------------------------
#--------------Handling Null values----------
#--------------------------------------------
 
# IS NULL OR NOT NULL
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
    data = [("James",None,"Smith","36636","M",60000), 
            ("Michael","Rose","","40288","M",70000),
            ("Robert","","Williams","42114","" ,400000),
            ("Maria","Anne","Jones","39192","F",500000),
            ("Jen","Mary","Brown",None,"F",0)]

    columns = ["first_name","middle_name","last_name","dob","gender","salary"]
    df = spark.createDataFrame(data = data, schema = columns)
    df.show()

    """    # None --> null (dataset) and "" -> empty string
    +----------+-----------+---------+-----+------+------+
    |first_name|middle_name|last_name|  dob|gender|salary|
    +----------+-----------+---------+-----+------+------+
    |     James|       null|    Smith|36636|     M| 60000|
    |   Michael|       Rose|         |40288|     M| 70000|
    |    Robert|           | Williams|42114|      |400000|
    |     Maria|       Anne|    Jones|39192|     F|500000|
    |       Jen|       Mary|    Brown| null|     F|     0|
    +----------+-----------+---------+-----+------+------+
    """

    df.where(df.middle_name.isNull()).show()
    """
    +----------+-----------+---------+-----+------+------+
    |first_name|middle_name|last_name|  dob|gender|salary|
    +----------+-----------+---------+-----+------+------+
    |     James|       null|    Smith|36636|     M| 60000|
    +----------+-----------+---------+-----+------+------+
    """

    df.where(df.middle_name.isNull() | df.dob.isNull() ).show() # multiple columns
    """
    +----------+-----------+---------+-----+------+------+
    |first_name|middle_name|last_name|  dob|gender|salary|
    +----------+-----------+---------+-----+------+------+
    |     James|       null|    Smith|36636|     M| 60000|
    |       Jen|       Mary|    Brown| null|     F|     0|
    +----------+-----------+---------+-----+------+------+
    """
    df.where(df.gender.isNull()).show() # no o/p


# Not Null
df.where(df.last_name.isNotNull()).show()


# Fill Null/None values - fillna() [alias for na.fill() ]
    fillna( value = , subset =)
    fillna ({ column_1 : value , column_2: value, .....})


    df.na.fill(value= 'XXX', subset=None).show() # all null/None values will be filled , can also call fill() without param names
        """
        +----------+-----------+---------+-----+------+------+
        |first_name|middle_name|last_name|  dob|gender|salary|
        +----------+-----------+---------+-----+------+------+
        |     James|        XXX|    Smith|36636|     M| 60000|
        |   Michael|       Rose|         |40288|     M| 70000|
        |    Robert|           | Williams|42114|      |400000|
        |     Maria|       Anne|    Jones|39192|     F|500000|
        |       Jen|       Mary|    Brown|  XXX|     F|     0|
        +----------+-----------+---------+-----+------+------+
        """

     df.na.fill(value= 'XXX', subset=['middle_name']).show() # fill for only specified clumns
        """
        +----------+-----------+---------+-----+------+------+
        |first_name|middle_name|last_name|  dob|gender|salary|
        +----------+-----------+---------+-----+------+------+
        |     James|        XXX|    Smith|36636|     M| 60000|
        |   Michael|       Rose|         |40288|     M| 70000|
        |    Robert|           | Williams|42114|      |400000|
        |     Maria|       Anne|    Jones|39192|     F|500000|
        |       Jen|       Mary|    Brown| null|     F|     0|
        +----------+-----------+---------+-----+------+------+
        """


    # fill diff value for each column    
    df.na.fill({"middle_name": 'XXX', "dob": "99999"}).show()
    # or,
    df.fillna({"middle_name": 'XXX', "dob": "99999"}).show()
    """
    +----------+-----------+---------+-----+------+------+
    |first_name|middle_name|last_name|  dob|gender|salary|
    +----------+-----------+---------+-----+------+------+
    |     James|        XXX|    Smith|36636|     M| 60000|
    |   Michael|       Rose|         |40288|     M| 70000|
    |    Robert|           | Williams|42114|      |400000|
    |     Maria|       Anne|    Jones|39192|     F|500000|
    |       Jen|       Mary|    Brown|99999|     F|     0|
    +----------+-----------+---------+-----+------+------+
    """

ps: fillna() - better to uas as function name similar to pandas 
    


 #-------------------------------------------
 #-------------------JOINS-------------------
 #-------------------------------------------
 
    help(join)
    # join(self,other, on= , how= )
    # null of no value  found 
    df = df1.join(df2,on=['id','mob_no'],how='inner')

    df_inner = df1.join(df2,df1.order_id==df2.order_id,'inner')  # inner,left,right
    
    # left join 
    df_left = df1.join(df2,df1.order_id==df2.order_id,'left').where ('order_item_order_id is null').show() # .count()
 
  
 #--------------------------------------------
 #----------------Sorting---------------------
 #--------------------------------------------
 - sort() or orderBy() can be used.
 
    df.sort('order_date','order_status').show()
    df.sort(['order_date','order_status'], ascending=[0,1]).show()
 
    # using orderBy
    df.orderBy("order_date","order_status")
    df.orderBy(col("order_date").asc(),col("order_status").asc())
    

    # using Raw SQL
    df.createTempView('orders_VW')
    spark.sql('select * from orders_VW order by order_status desc').show()
 
 
 
 #--------------------------------------------
 #-------------------AGGREGATE----------------
 #--------------------------------------------
    # list all availale functions , spark-shell can be used for this to print all available apis'
    # spark-shell
    # org.apache.spark.sql.functions.
    
    
        from pyspark.sql.functions import sum # check if required
        df.select(round(sum('order_price'),2)).show()
    
    
#GROUPBY
        df.groupBy('Order_id').sum('order_price').show()
    
        df.groupBy('Order_id').agg(sum('order_price')).show()
        df.groupBy('Order_id').agg(sum('order_price').alias('sum_price')).show()  # alias not allowed without agg
    
    
        df.groupBy('Order_id').count().show()
        
        df.groupBy("department","state").sum("salary","bonus")
    
    # to have alias and every function inside agg reuqires column name`
        df_orderItems.groupBy('Order_id').agg(count('order_id').alias('price_count')).show() 
 
 
 
 # SOME EXTRA AGGRAGTE FUNCTIONS in PYSPARK

simpleData = [("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
]
schema = ["employee_name", "department", "salary"]
df = spark.createDataFrame(data=simpleData, schema = schema)
df.printSchema()
df.show(truncate=False)
    
    # output :
    +-------------+----------+------+
    |employee_name|department|salary|
    +-------------+----------+------+
    |James        |Sales     |3000  |
    |Michael      |Marketing |4600  |
    |Robert       |Sales     |4100  |
    |Maria        |Finance   |3000  |
    |James        |Sales     |3000  |
    |Scott        |Finance   |3300  |
    +-------------+----------+------+

from pyspark.sql.functions import *    

1. count()
    df.select(count("salary")).show()  # Dataframe result
    #+-------------+
    #|count(salary)|
    #+-------------+
    #|            6|
    #+-------------+

    df.select('salary').count()  # Variable result


2. avg
    df.select(avg("salary")).show() 


3. sum
    df.select(sum("salary")).show()

    df.groupBy("department").sum("salary").show()
    #+----------+-----------+
    #|department|sum(salary)|
    #+----------+-----------+
    #|     Sales|      10100|
    #|   Finance|       6300|
    #| Marketing|       4600|
    #+----------+-----------+


4. max and min
    df.select(max("salary"),min("salary")).show() 
    #+-----------+-----------+
    #|max(salary)|min(salary)|
    #+-----------+-----------+
    #|       4600|       3000|
    #+-----------+-----------+    
 
 
 5. collect_list() - return all values of column in list , collect_set() - return distinct values of column in list
 
    df.select(collect_list("salary"),collect_set("salary")).show(truncate=False)
    #+------------------------------------+------------------------+
    #|collect_list(salary)                |collect_set(salary)     |
    #+------------------------------------+------------------------+
    #|[3000, 4600, 4100, 3000, 3000, 3300]|[4600, 3000, 4100, 3300]|
    #+------------------------------------+------------------------+


6. countDistinct : returns the count of distinct items in a group
    df.select(approx_count_distinct("salary")).show()  # in dataframe form
    #print(df.select(approx_count_distinct("salary")).collect()) -- return row --> [Row(approx_count_distinct(salary)=4)]
    print("approx_count_distinct: " + str(df.select(approx_count_distinct("salary")).collect()[0][0])) # o/p - 4


7. first and last : return first and last element from dataset, better to use orderBy first for desired result
    df.select(first("department"),last("department")).show()
    df.orderBy('department').select(first("department"),last("department")).show()
    
8. stats functions:
        - mean()
        - stdev(),variance()
        - kurtosis(),skewness()
        

 #---------------------------------------------------------------------
 #-----------------SET OPERATIONS ( Union/Intersect/Subtract )---------
 #---------------------------------------------------------------------
 - union() and unionall() works in same way but unionall() is deprecated from spark 2.0
 - no. of columns and schemas should be same.
 
    union_DF = df1.union(df2) # make practice of using union only

    union_DF = df1.intersect(df2) 

    union_DF = df1.subtract(df2) 
    
 - union() - can duplicate elements too , to avoid it use:
    distinct_union_DF = df1.union(df2).distinct()
 


 #--------------------------------------------
 #------Analytical or windowing functions-----
 #--------------------------------------------
 #import pyspark.sql.window or from pyspark.sql import window
 
 #syntax:
 rank().over(Window.partitionBy('column'))
 
 aggregations : sum,min,max,avg,count
 ranking:      rank,dense_rank, row_number
 windowing :   lead,lag
 
 
1. row_number()
    
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number
    
    windowSpec = Window.partitionBy("department").orderBy("salary")
    df.withColumn("row_number",row_number().over(windowSpec)).show(truncate=False)

    # in one line
    # df.withColumn("row_number",row_number().over(Window.partitionBy("department").orderBy("salary"))).show(truncate=False)


    #+-------------+----------+------+----------+
    #|employee_name|department|salary|row_number|
    #+-------------+----------+------+----------+
    #|James        |Sales     |3000  |1         |
    #|James        |Sales     |3000  |2         |
    #|Robert       |Sales     |4100  |3         |
    #|Maria        |Finance   |3000  |1         |
    #|Scott        |Finance   |3300  |2         |
    #|Michael      |Marketing |4600  |1         |
    #+-------------+----------+------+----------+
 
 
    # Ranking without partition
    df.withColumn("row_number",row_number().over(Window.orderBy("salary"))).show(truncate=False)
    
2. Rank() and dense_Rank() works in same way    
 
 # top N products per day
 
 
3. Percent_rank() - function returns a percentile ranking number which ranges from zero to one
                  - (rank - 1) / (total_rows - 1)
                  
    windowSpec = Window.orderBy("salary")
    df.withColumn("percent_rank",percent_rank().over(windowSpec)).show()
    
    #+-------------+----------+------+------------+
    #|employee_name|department|salary|percent_rank|
    #+-------------+----------+------+------------+
    #|        James|     Sales|  3000|         0.0|
    #|        Maria|   Finance|  3000|         0.0|
    #|        James|     Sales|  3000|         0.0|
    #|        Scott|   Finance|  3300|         0.6|
    #|       Robert|     Sales|  4100|         0.8|
    #|      Michael| Marketing|  4600|         1.0|
    #+-------------+----------+------+------------+                  
 
 
 
 4. ntile(n) - divides the dataset into n groups
    
    windowSpec = Window.orderBy("salary")
    df.withColumn("ntile",ntile(4).over(windowSpec)).show().
 
 
 5. Lead() /LAG()

    windowSpec = Window.orderBy("salary")
    df.withColumn("lead",lead("salary",2).over(windowSpec)).show()

    #+-------------+----------+------+----+
    #|employee_name|department|salary|lead|
    #+-------------+----------+------+----+
    #|        James|     Sales|  3000|3000|
    #|        Maria|   Finance|  3000|3300|
    #|        James|     Sales|  3000|4100|
    #|        Scott|   Finance|  3300|4600|
    #|       Robert|     Sales|  4100|null|
    #|      Michael| Marketing|  4600|null|
    #+-------------+----------+------+----+
 
 
 
 
 #--------------------------------------------
 #------ Pyspark user-define functions -------
 #--------------------------------------------
 
 
 
 
 
 
 
 
 
 #--------------------------------------------
 #-----------Practice question (similar to RDD)
 #--------------------------------------------
 
 
 
 
 
# ------Launch spark sql cli-----
 --logon to gateway node of cluster
        spark-sql
        spark-sql > select ........
        spark-sql --help
        
-- Spark sql properties        
        /etc/spark2/conf
        or , view spark-sql properties with SET; on spark-sql cli
        
-- run os commands from spark-sql cli   
        ! ls -l
        ! hdfs fs -ls /user/...
        
-- spark wraheouse directoey
Spark sql database - is just like file
            spark-sql> SET spark.sql.warehouse.dir;
                        show tables;
                        select current_database();
            hdfs dfs -ls /user/cloudera/warehouse | grep <db_name> # wull give file with .db 
            # if you print above .db file it will give you all tables files
            # if you want to see partition tables just type ..../..db/order_part # it will files with partition


-- Manage spark database -- look at video 157
                - create/drop db
                - show/use db;
                - show tables
                - create db with location
                - mostly work same as of hive
                
-- Manage spark tables -- look at video 158            
        


-- create both spark and sc explicitly
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName('Demo').getOrCreate()
        sc = spark.sparkContext  # spark context from sparkSession