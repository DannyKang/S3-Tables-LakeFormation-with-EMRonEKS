#!/usr/bin/env python3
"""
Marketing Partner Analysis - 마케팅 파트너 분석
Lake Formation FGAC 데모용 - 강남구 20-30대만 접근 (~1,650건)
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
        .appName("MarketingPartner-TargetAccess-Analysis") \
        .config("spark.sql.catalog.s3tables_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.s3tables_catalog.catalog-impl", "org.apache.iceberg.aws.s3.S3TablesCatalog") \
        .config("spark.sql.catalog.s3tables_catalog.warehouse", f"s3://{table_bucket}/") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .getOrCreate()

def main():
    print("=== Marketing Partner Analysis 시작 ===")
    print("역할: 마케팅 파트너 - 강남구 20-30대 타겟 고객만 접근")
    
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
        print("🔒 접근 제한: 강남구 + 20-30대만 (birth_year 직접 접근 불가)")
        
        # 데이터 스키마 확인 (접근 가능한 컬럼만)
        print("\n2. 접근 가능한 데이터 스키마:")
        df.printSchema()
        
        # 지역 확인 (강남구만 있어야 함)
        print(f"\n3. 접근 가능한 지역 확인:")
        district_check = df.groupBy("district").count().orderBy(desc("count"))
        district_check.show()
        
        # 타겟 고객 성별 분포
        print(f"\n4. 타겟 고객 성별 분포:")
        gender_distribution = df.groupBy("gender") \
                                .agg(count("*").alias("이용횟수"),
                                     avg("usage_min").alias("평균_이용시간"),
                                     avg("distance_meter").alias("평균_이동거리")) \
                                .orderBy(desc("이용횟수"))
        
        gender_distribution.show()
        
        # 타겟 고객 이용 패턴 - 시간대별
        print(f"\n5. 타겟 고객 시간대별 이용 패턴:")
        hourly_pattern = df.withColumn("hour", hour("rental_date")) \
                           .groupBy("hour") \
                           .agg(count("*").alias("이용횟수"),
                                avg("usage_min").alias("평균_이용시간")) \
                           .orderBy("hour")
        
        hourly_pattern.show(24)
        
        # 타겟 고객 요일별 이용 패턴
        print(f"\n6. 타겟 고객 요일별 이용 패턴:")
        weekday_pattern = df.withColumn("weekday", date_format("rental_date", "EEEE")) \
                            .groupBy("weekday") \
                            .agg(count("*").alias("이용횟수"),
                                 avg("usage_min").alias("평균_이용시간"),
                                 avg("distance_meter").alias("평균_이동거리")) \
                            .orderBy(desc("이용횟수"))
        
        weekday_pattern.show()
        
        # 인기 정거장 분석 (마케팅 포인트)
        print(f"\n7. 타겟 고객 인기 정거장 (상위 10개):")
        popular_stations = df.groupBy("station_name", "station_id") \
                             .agg(count("*").alias("이용횟수"),
                                  countDistinct("rental_id").alias("고유_이용자수"),
                                  avg("usage_min").alias("평균_이용시간"),
                                  avg("distance_meter").alias("평균_이동거리")) \
                             .orderBy(desc("이용횟수")) \
                             .limit(10)
        
        popular_stations.show(truncate=False)
        
        # 이용 시간 선호도 분석
        print(f"\n8. 타겟 고객 이용 시간 선호도:")
        usage_preference = df.withColumn("usage_category",
                                       when(col("usage_min") <= 10, "짧은이용(≤10분)")
                                       .when(col("usage_min") <= 20, "보통이용(11-20분)")
                                       .when(col("usage_min") <= 30, "긴이용(21-30분)")
                                       .otherwise("매우긴이용(>30분)")) \
                            .groupBy("usage_category") \
                            .agg(count("*").alias("이용횟수"),
                                 avg("distance_meter").alias("평균_이동거리")) \
                            .orderBy(desc("이용횟수"))
        
        usage_preference.show()
        
        # 이동 거리 선호도 분석
        print(f"\n9. 타겟 고객 이동 거리 선호도:")
        distance_preference = df.withColumn("distance_category",
                                          when(col("distance_meter") <= 1000, "근거리(≤1km)")
                                          .when(col("distance_meter") <= 2000, "중거리(1-2km)")
                                          .when(col("distance_meter") <= 3000, "장거리(2-3km)")
                                          .otherwise("초장거리(>3km)")) \
                               .groupBy("distance_category") \
                               .agg(count("*").alias("이용횟수"),
                                    avg("usage_min").alias("평균_이용시간")) \
                               .orderBy(desc("이용횟수"))
        
        distance_preference.show()
        
        # 사용자 유형별 분석
        print(f"\n10. 타겟 고객 사용자 유형:")
        user_type_analysis = df.groupBy("user_type") \
                               .agg(count("*").alias("이용횟수"),
                                    avg("usage_min").alias("평균_이용시간"),
                                    avg("distance_meter").alias("평균_이동거리")) \
                               .orderBy(desc("이용횟수"))
        
        user_type_analysis.show()
        
        # 마케팅 인사이트 - 피크 시간대
        print(f"\n11. 마케팅 인사이트 - 피크 시간대:")
        peak_hours = df.withColumn("hour", hour("rental_date")) \
                       .withColumn("time_category",
                                 when(col("hour").between(7, 9), "출근시간")
                                 .when(col("hour").between(12, 14), "점심시간")
                                 .when(col("hour").between(18, 20), "퇴근시간")
                                 .when(col("hour").between(21, 23), "저녁시간")
                                 .otherwise("기타시간")) \
                       .groupBy("time_category") \
                       .agg(count("*").alias("이용횟수"),
                            avg("usage_min").alias("평균_이용시간")) \
                       .orderBy(desc("이용횟수"))
        
        peak_hours.show()
        
        # 타겟 고객 행동 요약
        print(f"\n12. 타겟 고객 행동 요약:")
        behavior_summary = df.agg(
            count("*").alias("총_이용횟수"),
            countDistinct("station_id").alias("이용_정거장수"),
            avg("usage_min").alias("평균_이용시간"),
            avg("distance_meter").alias("평균_이동거리"),
            expr("percentile_approx(usage_min, 0.5)").alias("중앙값_이용시간"),
            expr("percentile_approx(distance_meter, 0.5)").alias("중앙값_이동거리")
        )
        
        behavior_summary.show()
        
        print(f"\n=== Marketing Partner Analysis 완료 ===")
        print(f"✅ 분석 완료: {total_records:,}건 (강남구 20-30대만)")
        print(f"🔒 권한: 강남구 + 20-30대 타겟 고객만 (birth_year 직접 접근 불가)")
        print(f"📊 역할: 타겟 마케팅 분석 및 고객 세그먼트 분석")
        
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
