# 1. 필요한 라이브러리들을 불러옵니다.
import os
from dotenv import load_dotenv
import dart_fss as dart
import pandas as pd

# 2. .env 파일에 저장된 환경 변수들을 불러옵니다.
load_dotenv()
print("--- 환경 변수 로드 완료 ---")

# 3. OS 환경에서 DART_API_KEY를 가져와 변수에 저장합니다.
api_key = os.getenv("DART_API_KEY")

if api_key:
    print(f"✅ API 키를 성공적으로 불러왔습니다. (키 일부: ...{api_key[-6:]})")
else:
    print("🚨 .env 파일에서 DART_API_KEY를 찾을 수 없습니다.")
    exit()

# 4. python-dart 라이브러리에 API 키를 등록합니다.
dart.set_api_key(api_key=api_key)
print("--- DART API 키 설정 완료 ---")

# 5. 작업할 회사 목록이 담긴 CSV 파일을 불러옵니다.
try:
    df_companies = pd.read_csv('data/companies.csv')
    print("✅ 'data/companies.csv' 파일을 성공적으로 불러왔습니다.")
    print("--- 작업 대상 기업 (상위 5개) ---")
    print(df_companies.head())
    print("--------------------")
except FileNotFoundError:
    print("🚨 'data/companies.csv' 파일을 찾을 수 없습니다.")
    exit()

# 6. 모든 준비 완료! 최종 API 통신 테스트를 시작합니다.
print("\n--- API 통신 테스트 시작 ---")
try:
    # CSV 파일의 첫 번째 회사 이름을 가져옵니다.
    first_company_name = df_companies.iloc[0]['corp_name']
    
    print(f"🔬 '{first_company_name}'의 DART 고유번호를 조회합니다...")
    
    # ★★★ 핵심 변경점 1: 회사 이름으로 DART 고유번호 찾기 ★★★
    corps = dart.corp.find_by_corp_name(first_company_name, exactly=True)
    if not corps:
        print(f"🚨 DART에서 '{first_company_name}'을 찾을 수 없습니다. 회사 이름을 확인해주세요.")
        exit()
    
    # 찾은 회사 정보에서 고유번호를 추출합니다.
    target_corp = corps[0]
    target_corp_code = target_corp.corp_code
    print(f"✅ '{target_corp.corp_name}'의 고유번호 '{target_corp_code}'를 찾았습니다.")

    print(f"\n🔬 '{target_corp.corp_name}'의 기업 개황 정보를 조회합니다...")
    
    # ★★★ 핵심 변경점 2: 찾은 진짜 고유번호로 API 요청 ★★★
    corp_info = dart.api.filings.get_corp_info(corp_code=target_corp_code)

    # ★★★ 핵심 변경점 3: 서버 응답이 dict(사전)인지 확인하여 에러 처리 ★★★
    if isinstance(corp_info, dict):
        # 응답이 사전 형태라면, 에러 메시지일 가능성이 높습니다.
        print(f"🚨 DART 서버로부터 응답을 받았으나, 데이터가 아닌 상태 메시지입니다.")
        print(f"-> 서버 응답: {corp_info}")
    else:
        # 응답이 객체 형태라면, 성공적으로 정보를 가져온 것입니다.
        print("✅ API 통신 성공!")
        print(f"-> '{corp_info.corp_name}'의 설립일: {corp_info.est_dt}, 대표이사: {corp_info.ceo_nm}")

except Exception as e:
    print(f"🚨 API 통신 중 예상치 못한 오류가 발생했습니다: {e}")
    