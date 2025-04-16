from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import matplotlib.pyplot as plt
import seaborn as sns
from pyecharts.charts import Map
from pyecharts import options as opts

# 初始化Spark
spark = SparkSession.builder \
    .appName("UserAnalysis") \
    .config("spark.sql.files.maxPartitionBytes", "256MB") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

# 增强的省份提取正则
province_pattern = r"^([\u4e00-\u9fa5]{2,7}?(省|自治区|市|特别行政区))"

# 统一模式定义
purchase_schema = StructType([
    StructField("average_price", DoubleType()),
    StructField("category", StringType()),
    StructField("items", ArrayType(StructType([StructField("id", IntegerType())])))
])

main_schema = StructType([
    StructField("timestamp", TimestampType()),
    StructField("user_name", StringType()),
    StructField("chinese_name", StringType()),
    StructField("income", DoubleType()),
    StructField("chinese_address", StringType()),
    StructField("purchase_history", purchase_schema),
    StructField("is_active", BooleanType()),
    StructField("registration_date", DateType()),
    StructField("credit_score", IntegerType()),
    StructField("phone_number", StringType())
])

def load_and_preprocess(file_list):
    """
    多CSV文件加载与预处理
    参数：file_list - CSV文件路径列表
    """
    # 并行读取多个CSV
    df = spark.read.csv(
        file_list,
        schema=main_schema,
        header=True,
        timestampFormat="yyyy-MM-dd HH:mm:ss",
        dateFormat="yyyy-MM-dd"
    )
    
    # 数据清洗管道
    df_clean = (
        df
        # 提取标准省份名称
        .withColumn("province_raw", regexp_extract(col("chinese_address"), province_pattern, 1))
        # 统一省份命名
        .withColumn("province", 
                   expr("""
                       CASE province_raw
                           WHEN '内蒙古自治区' THEN '内蒙古'
                           WHEN '西藏自治区' THEN '西藏'
                           WHEN '广西壮族自治区' THEN '广西'
                           WHEN '宁夏回族自治区' THEN '宁夏'
                           WHEN '新疆维吾尔自治区' THEN '新疆'
                           WHEN '北京市' THEN '北京'
                           WHEN '天津市' THEN '天津'
                           WHEN '上海市' THEN '上海'
                           WHEN '重庆市' THEN '重庆'
                           WHEN '香港特别行政区' THEN '香港'
                           WHEN '澳门特别行政区' THEN '澳门'
                           ELSE regexp_replace(province_raw, '(省|市|特别行政区)', '')
                       END
                   """))
        # 购买行为解析
        .withColumn("purchase_category", col("purchase_history.category"))
        .withColumn("avg_purchase_price", col("purchase_history.average_price"))
        .withColumn("purchase_count", size(col("purchase_history.items")))
        # 时间特征
        .withColumn("registration_year", year(col("registration_date")))
        .withColumn("days_since_registration", datediff(current_date(), col("registration_date")))
    )
    
    return df_clean

def generate_heatmap(df, metric='user_count', title='用户分布热力图'):
    """
    生成PyEcharts热力图
    参数：
        df - 包含province和metric列的Pandas DataFrame
        metric - 度量字段名
        title - 图表标题
    """
    # 转换为PyEcharts需要的格式
    value_list = df[['province', metric]].values.tolist()
    
    # 生成地图
    m = Map(init_opts=opts.InitOpts(width="1200px", height="800px"))
    m.add(series_name=title, 
          data_pair=value_list,
          maptype="china",
          is_map_symbol_show=False)
    
    m.set_global_opts(
        title_opts=opts.TitleOpts(title=title),
        visualmap_opts=opts.VisualMapOpts(
            max_=df[metric].max(),
            is_piecewise=True,
            pos_top="middle",
            pos_left="left",
            orient="vertical"
        )
    )
    return m

# 主分析流程
if __name__ == "__main__":
    # 输入CSV文件列表（示例路径）
    csv_files = [
        "csv_file\\test.csv",
        # ...添加所有CSV路径
    ]
    
    # 数据加载
    df = load_and_preprocess(csv_files)
    
    # 地理分布分析
    province_dist = df.groupBy("province").agg(
        count("user_name").alias("user_count"),
        sum("income").alias("total_income"),
        avg("credit_score").alias("avg_credit")
    ).orderBy("user_count", ascending=False)

    # 转换为Pandas DataFrame
    province_pd = province_dist.toPandas()
    
    # 生成热力图（用户数分布）
    user_map = generate_heatmap(province_pd, 'user_count', '用户数量分布')
    income_map = generate_heatmap(province_pd, 'total_income', '总收入分布')
    credit_map = generate_heatmap(province_pd, 'avg_credit', '平均信用分分布')
    
    # 其他分析流程...
    
    spark.stop()