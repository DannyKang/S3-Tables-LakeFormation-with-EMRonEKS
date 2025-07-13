#!/usr/bin/env python3
"""
Marketing Partner Role - 강남구 20대 타겟 마케팅 분석
목적: 강남구 20대 고객 대상 마케팅 캠페인 기획 및 타겟 분석
제한: district='강남구' AND age_group='20대' 데이터만 접근, 
      payment_amount/user_id/rental_duration 컬럼 접근 불가
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys

def create_spark_session():
    """Lake Formation 통합 Spark 세션 생성"""
    return SparkSession.builder \
        .appName("MarketingPartner-SeoulBikeAnalysis") \
        .config("spark.sql.catalog.seoul_bike_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.seoul_bike_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.seoul_bike_catalog.warehouse", "s3://seoul-bike-rental-data-202506/") \
        .config("spark.sql.catalog.seoul_bike_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.WebIdentityTokenCredentialsProvider") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .getOrCreate()

def analyze_target_demographics(spark):
    """강남구 20대 타겟 고객 분석"""
    print("=== 강남구 20대 타겟 고객 분석 ===")
    
    # Lake Formation FGAC에 의해 강남구 20대 데이터만 자동 필터링됨
    df = spark.sql("""
        SELECT rental_id, station_id, station_name, district, rental_date, return_date,
               user_type, age_group, gender
        FROM seoul_bike_catalog.bike_db.bike_rental_data
        WHERE rental_date >= '2025-06-01' AND rental_date <= '2025-06-30'
    """)
    
    # 데이터 확인 - 강남구 20대 데이터만 보여야 함
    print("접근 가능한 데이터 확인:")
    df.select("district", "age_group").distinct().show()
    
    total_target_customers = df.count()
    print(f"타겟 고객 대여 건수: {total_target_customers:,}")
    
    # 성별 분포 분석
    gender_analysis = df.groupBy("gender") \
        .agg(count("*").alias("rental_count")) \
        .withColumn("percentage", round((col("rental_count") * 100.0 / total_target_customers), 2)) \
        .orderBy(desc("rental_count"))
    
    print("\n=== 타겟 고객 성별 분포 ===")
    gender_analysis.show()
    
    return df

def analyze_station_preferences(df):
    """정거장별 이용 선호도 분석"""
    print("\n=== 강남구 20대 정거장 이용 선호도 ===")
    
    # 정거장별 이용 현황
    station_preference = df.groupBy("station_id", "station_name") \
        .agg(
            count("*").alias("usage_count"),
            countDistinct("rental_date").alias("usage_days")
        ) \
        .orderBy(desc("usage_count"))
    
    print("정거장별 이용 현황:")
    station_preference.show(20, truncate=False)
    
    # 성별 정거장 선호도
    gender_station_preference = df.groupBy("station_name", "gender") \
        .agg(count("*").alias("usage_count")) \
        .orderBy("station_name", desc("usage_count"))
    
    print("성별 정거장 선호도:")
    gender_station_preference.show(30, truncate=False)
    
    return station_preference

def analyze_usage_patterns(df):
    """이용 패턴 분석"""
    print("\n=== 강남구 20대 이용 패턴 분석 ===")
    
    # 사용자 유형별 분석
    user_type_analysis = df.groupBy("user_type", "gender") \
        .agg(count("*").alias("usage_count")) \
        .orderBy("user_type", desc("usage_count"))
    
    print("사용자 유형별 성별 분포:")
    user_type_analysis.show()
    
    # 시간대별 이용 패턴
    hourly_pattern = df.withColumn("hour", hour("rental_date")) \
        .groupBy("hour", "gender") \
        .agg(count("*").alias("usage_count")) \
        .orderBy("hour", "gender")
    
    print("시간대별 성별 이용 패턴:")
    hourly_pattern.show(48)
    
    # 요일별 패턴
    df_with_weekday = df.withColumn("weekday", date_format("rental_date", "EEEE")) \
                       .withColumn("day_of_week", dayofweek("rental_date"))
    
    weekday_pattern = df_with_weekday.groupBy("day_of_week", "weekday", "gender") \
        .agg(count("*").alias("usage_count")) \
        .orderBy("day_of_week", "gender")
    
    print("요일별 성별 이용 패턴:")
    weekday_pattern.show()

def generate_marketing_insights(df):
    """마케팅 인사이트 생성"""
    print("\n=== 마케팅 인사이트 및 캠페인 제안 ===")
    
    # 피크 시간대 분석
    peak_hours = df.withColumn("hour", hour("rental_date")) \
        .groupBy("hour") \
        .agg(count("*").alias("usage_count")) \
        .orderBy(desc("usage_count"))
    
    print("피크 시간대 TOP 5:")
    peak_hours.show(5)
    
    # 인기 정거장 TOP 5
    popular_stations = df.groupBy("station_name") \
        .agg(count("*").alias("usage_count")) \
        .orderBy(desc("usage_count"))
    
    print("인기 정거장 TOP 5:")
    popular_stations.show(5, truncate=False)
    
    # 사용자 유형별 선호 시간대
    user_time_preference = df.withColumn("hour", hour("rental_date")) \
        .withColumn("time_period",
            when(col("hour").between(6, 11), "오전")
            .when(col("hour").between(12, 17), "오후")
            .when(col("hour").between(18, 23), "저녁")
            .otherwise("새벽")
        ) \
        .groupBy("user_type", "time_period", "gender") \
        .agg(count("*").alias("usage_count")) \
        .orderBy("user_type", desc("usage_count"))
    
    print("사용자 유형별 시간대 선호도:")
    user_time_preference.show(20)

def generate_campaign_recommendations(df):
    """캠페인 추천안 생성"""
    print("\n=== 마케팅 캠페인 추천안 ===")
    
    # 데이터 기반 인사이트 수집
    total_rentals = df.count()
    unique_stations = df.select("station_id").distinct().count()
    
    # 성별 분포
    gender_dist = df.groupBy("gender").count().collect()
    male_count = next((row['count'] for row in gender_dist if row['gender'] == 'M'), 0)
    female_count = next((row['count'] for row in gender_dist if row['gender'] == 'F'), 0)
    
    # 사용자 유형 분포
    user_type_dist = df.groupBy("user_type").count().collect()
    regular_count = next((row['count'] for row in user_type_dist if row['user_type'] == '정기'), 0)
    casual_count = next((row['count'] for row in user_type_dist if row['user_type'] == '일반'), 0)
    tourist_count = next((row['count'] for row in user_type_dist if row['user_type'] == '관광객'), 0)
    
    print("📊 타겟 고객 프로필 요약:")
    print(f"   • 총 대여 건수: {total_rentals}건")
    print(f"   • 이용 정거장 수: {unique_stations}개")
    print(f"   • 남성: {male_count}건 ({male_count/total_rentals*100:.1f}%)")
    print(f"   • 여성: {female_count}건 ({female_count/total_rentals*100:.1f}%)")
    print(f"   • 정기 이용자: {regular_count}건")
    print(f"   • 일반 이용자: {casual_count}건")
    print(f"   • 관광객: {tourist_count}건")
    
    print("\n🎯 추천 마케팅 캠페인:")
    
    if male_count > female_count:
        print("1. 남성 타겟 캠페인")
        print("   • '강남 직장인 남성을 위한 출퇴근 자전거 패키지'")
        print("   • 오전/저녁 시간대 집중 프로모션")
    else:
        print("1. 여성 타겟 캠페인")
        print("   • '강남 여성을 위한 안전한 자전거 라이딩'")
        print("   • 주말 레저 활동 연계 프로모션")
    
    if regular_count > casual_count:
        print("\n2. 정기 이용자 확대 캠페인")
        print("   • '20대 정기 이용권 할인 이벤트'")
        print("   • 친구 추천 시 추가 혜택 제공")
    else:
        print("\n2. 일반 이용자 전환 캠페인")
        print("   • '첫 정기권 50% 할인 이벤트'")
        print("   • 무료 체험 기간 제공")
    
    if tourist_count > 0:
        print("\n3. 관광객 타겟 캠페인")
        print("   • '강남 투어 자전거 패키지'")
        print("   • 관광 명소 연계 할인 혜택")
    
    print("\n📍 추천 광고 위치:")
    top_stations = df.groupBy("station_name").count().orderBy(desc("count")).limit(3).collect()
    for i, station in enumerate(top_stations, 1):
        print(f"   {i}. {station['station_name']} ({station['count']}건)")

def test_restricted_access(spark):
    """제한된 컬럼 접근 테스트"""
    print("\n=== 제한된 컬럼 접근 테스트 ===")
    
    # 결제 정보 접근 시도 (실패해야 함)
    print("1. 결제 정보 접근 테스트 (실패 예상):")
    try:
        payment_data = spark.sql("""
            SELECT station_name, sum(payment_amount) as total_revenue
            FROM seoul_bike_catalog.bike_db.bike_rental_data
            GROUP BY station_name
            LIMIT 10
        """)
        payment_data.show()
        print("⚠️  경고: 결제 정보에 접근할 수 있습니다. 권한 설정을 확인하세요.")
    except Exception as e:
        print(f"✅ 예상된 결과: 결제 정보 접근 차단됨 - {str(e)}")
    
    # 개인정보 접근 시도 (실패해야 함)
    print("\n2. 개인정보 접근 테스트 (실패 예상):")
    try:
        user_data = spark.sql("""
            SELECT user_id, count(*) as rental_count
            FROM seoul_bike_catalog.bike_db.bike_rental_data
            GROUP BY user_id
            LIMIT 10
        """)
        user_data.show()
        print("⚠️  경고: 개인정보에 접근할 수 있습니다. 권한 설정을 확인하세요.")
    except Exception as e:
        print(f"✅ 예상된 결과: 개인정보 접근 차단됨 - {str(e)}")
    
    # 대여 시간 정보 접근 시도 (실패해야 함)
    print("\n3. 대여 시간 정보 접근 테스트 (실패 예상):")
    try:
        duration_data = spark.sql("""
            SELECT station_name, avg(rental_duration) as avg_duration
            FROM seoul_bike_catalog.bike_db.bike_rental_data
            GROUP BY station_name
            LIMIT 10
        """)
        duration_data.show()
        print("⚠️  경고: 대여 시간 정보에 접근할 수 있습니다. 권한 설정을 확인하세요.")
    except Exception as e:
        print(f"✅ 예상된 결과: 대여 시간 정보 접근 차단됨 - {str(e)}")

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        print("Marketing Partner Role - 강남구 20대 타겟 마케팅 분석 시작")
        print("🎯 타겟: 강남구 거주/이용 20대 고객")
        print("📊 목적: 타겟 마케팅 캠페인 기획 및 고객 분석")
        print("🔒 제한: Lake Formation FGAC에 의해 강남구 20대 데이터만 접근 가능")
        
        # 타겟 고객 분석
        df = analyze_target_demographics(spark)
        
        # 정거장 선호도 분석
        station_pref = analyze_station_preferences(df)
        
        # 이용 패턴 분석
        analyze_usage_patterns(df)
        
        # 마케팅 인사이트 생성
        generate_marketing_insights(df)
        
        # 캠페인 추천안 생성
        generate_campaign_recommendations(df)
        
        # 제한된 접근 테스트
        test_restricted_access(spark)
        
        # 결과 저장
        output_path = "s3://seoul-bike-analytics-results/marketing-partner/monthly-report-202506/"
        
        # 마케팅 타겟 요약 저장
        marketing_summary = df.groupBy("station_id", "station_name", "user_type", "gender") \
            .agg(count("*").alias("target_count")) \
            .orderBy(desc("target_count"))
        
        marketing_summary.coalesce(1) \
            .write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(f"{output_path}/marketing_target_summary")
        
        print(f"\n📁 마케팅 분석 결과가 {output_path}에 저장되었습니다.")
        
        # 최종 요약
        print("\n" + "="*60)
        print("🎯 마케팅 파트너 분석 완료")
        print("="*60)
        print("✅ 강남구 20대 타겟 고객 프로파일링 완료")
        print("✅ 정거장별 이용 선호도 분석 완료")
        print("✅ 시간대/요일별 이용 패턴 분석 완료")
        print("✅ 맞춤형 마케팅 캠페인 추천안 생성 완료")
        print("✅ 데이터 접근 제한 검증 완료")
        
    except Exception as e:
        print(f"❌ 오류 발생: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
