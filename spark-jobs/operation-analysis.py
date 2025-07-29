#!/usr/bin/env python3
"""
Operation Analysis - 운영 데이터 분석
Lake Formation FGAC 데모용 - 전체 구 운영 데이터 (개인정보 제외)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys

def create_spark_session():
    """S3 Tables(Iceberg) 지원 Spark 세션 생성"""
    import os
    table_bucket = os.getenv('TABLE_BUCKET_NAME', 'seoul-bike-demo-2025')
    
    return SparkSession.builder \
        .appName("Operation-SystemAccess-Analysis") \
        .config("spark.sql.catalog.s3tables_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.s3tables_catalog.catalog-impl", "org.apache.iceberg.aws.s3.S3TablesCatalog") \
        .config("spark.sql.catalog.s3tables_catalog.warehouse", f"s3://{table_bucket}/") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .getOrCreate()

def main():
    print("=== Operation Analysis 시작 ===")
    print("역할: 운영팀 - 전체 구 운영 데이터 (개인정보 제외)")
    
    spark = create_spark_session()
    
    try:
        # S3 Tables에서 데이터 읽기
        print("\n1. S3 Tables에서 데이터 로드 중...")
        spark_conf = spark.sparkContext.getConf()
        namespace = spark_conf.get('spark.app.s3tables.namespace', 'bike_db')
        table_name = spark_conf.get('spark.app.s3tables.table', 'bike_rental_data')
        
        print(f"   네임스페이스: {namespace}")
        print(f"   테이블명: {table_name}")
        
        df = spark.read.table(f"s3tables_catalog.{namespace}.{table_name}")
        
        total_records = df.count()
        print(f"✅ Lake Formation 필터링 후 레코드 수: {total_records:,}건")
        print("🔒 접근 제한: 개인정보(birth_year, gender) 제외")
        
        # 데이터 스키마 확인 (접근 가능한 컬럼만)
        print("\n2. 접근 가능한 데이터 스키마:")
        df.printSchema()
        
        # 구별 운영 현황
        print(f"\n3. 구별 운영 현황:")
        district_ops = df.groupBy("district") \
                         .agg(count("*").alias("총_이용횟수"),
                              countDistinct("station_id").alias("정거장_수"),
                              avg("usage_min").alias("평균_이용시간"),
                              avg("distance_meter").alias("평균_이동거리")) \
                         .orderBy(desc("총_이용횟수"))
        
        district_ops.show(truncate=False)
        
        # 정거장별 운영 효율성 분석
        print(f"\n4. 정거장별 운영 효율성 (상위 20개):")
        station_efficiency = df.groupBy("station_id", "station_name", "district") \
                               .agg(count("*").alias("이용횟수"),
                                    avg("usage_min").alias("평균_이용시간"),
                                    avg("distance_meter").alias("평균_이동거리"),
                                    min("usage_min").alias("최소_이용시간"),
                                    max("usage_min").alias("최대_이용시간")) \
                               .orderBy(desc("이용횟수")) \
                               .limit(20)
        
        station_efficiency.show(truncate=False)
        
        # 시간대별 운영 부하 분석
        print(f"\n5. 시간대별 운영 부하:")
        hourly_load = df.withColumn("hour", hour("rental_date")) \
                        .groupBy("hour") \
                        .agg(count("*").alias("이용횟수"),
                             avg("usage_min").alias("평균_이용시간"),
                             countDistinct("station_id").alias("활성_정거장수")) \
                        .orderBy("hour")
        
        hourly_load.show(24)
        
        # 요일별 운영 패턴
        print(f"\n6. 요일별 운영 패턴:")
        weekday_ops = df.withColumn("weekday", date_format("rental_date", "EEEE")) \
                        .groupBy("weekday") \
                        .agg(count("*").alias("이용횟수"),
                             countDistinct("station_id").alias("활성_정거장수"),
                             avg("usage_min").alias("평균_이용시간"),
                             avg("distance_meter").alias("평균_이동거리")) \
                        .orderBy(desc("이용횟수"))
        
        weekday_ops.show()
        
        # 이용 시간 분포 (운영 최적화용)
        print(f"\n7. 이용 시간 분포 분석:")
        usage_distribution = df.withColumn("usage_category",
                                         when(col("usage_min") <= 5, "단거리(≤5분)")
                                         .when(col("usage_min") <= 15, "중거리(6-15분)")
                                         .when(col("usage_min") <= 30, "장거리(16-30분)")
                                         .when(col("usage_min") <= 60, "초장거리(31-60분)")
                                         .otherwise("특수(>60분)")) \
                              .groupBy("usage_category") \
                              .agg(count("*").alias("이용횟수"),
                                   avg("distance_meter").alias("평균_이동거리")) \
                              .orderBy(desc("이용횟수"))
        
        usage_distribution.show()
        
        # 이동 거리 분포 (자전거 재배치 계획용)
        print(f"\n8. 이동 거리 분포 분석:")
        distance_distribution = df.withColumn("distance_category",
                                            when(col("distance_meter") <= 500, "근거리(≤500m)")
                                            .when(col("distance_meter") <= 1500, "중거리(501-1500m)")
                                            .when(col("distance_meter") <= 3000, "장거리(1501-3000m)")
                                            .when(col("distance_meter") <= 5000, "초장거리(3001-5000m)")
                                            .otherwise("특수(>5000m)")) \
                                 .groupBy("distance_category") \
                                 .agg(count("*").alias("이용횟수"),
                                      avg("usage_min").alias("평균_이용시간")) \
                                 .orderBy(desc("이용횟수"))
        
        distance_distribution.show()
        
        # 정거장 이용률 분석 (재배치 우선순위)
        print(f"\n9. 정거장 이용률 분석 (하위 10개 - 재배치 필요):")
        low_usage_stations = df.groupBy("station_id", "station_name", "district") \
                               .agg(count("*").alias("이용횟수"),
                                    avg("usage_min").alias("평균_이용시간")) \
                               .orderBy("이용횟수") \
                               .limit(10)
        
        low_usage_stations.show(truncate=False)
        
        # 시스템 성능 지표
        print(f"\n10. 시스템 성능 지표:")
        performance_metrics = df.agg(
            count("*").alias("총_이용횟수"),
            countDistinct("station_id").alias("총_정거장수"),
            countDistinct("district").alias("서비스_구수"),
            avg("usage_min").alias("평균_이용시간"),
            avg("distance_meter").alias("평균_이동거리"),
            expr("percentile_approx(usage_min, 0.95)").alias("95퍼센타일_이용시간"),
            expr("percentile_approx(distance_meter, 0.95)").alias("95퍼센타일_이동거리")
        )
        
        performance_metrics.show()
        
        print(f"\n=== Operation Analysis 완료 ===")
        print(f"✅ 분석 완료: {total_records:,}건 (전체 구)")
        print(f"🔒 권한: 운영 데이터만 접근 (개인정보 제외)")
        print(f"📊 역할: 시스템 운영 최적화 및 정거장 관리")
        
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
