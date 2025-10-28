"""
DART 데이터 대량 수집 파이프라인 (DART API 직접 호출)
"""
import os
import time
import json
import logging
import requests
from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd
from dotenv import load_dotenv
import xml.etree.ElementTree as ET

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data/collection.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

print("="*60)
print("DART 데이터 수집 시작")
print("="*60)

# 환경 변수 로드
load_dotenv()
API_KEY = os.getenv('DART_API_KEY')
if not API_KEY:
    print("❌ 오류: .env 파일에서 DART_API_KEY를 찾을 수 없습니다")
    exit(1)

print("✓ API 키 설정 완료")

# DART API 엔드포인트
BASE_URL = "https://opendart.fss.or.kr/api"

# 보고서 타입 정의 (검색용 키워드)
REPORT_TYPES = {
    '사업보고서': ['사업보고서'],
    '반기보고서': ['반기보고서'],
    '분기보고서': ['분기보고서'],
}

class DartCollector:
    def __init__(self, companies_csv: str = 'data/companies.csv'):
        print(f"\n📂 {companies_csv} 파일 읽기 중...")
        self.companies_df = pd.read_csv(companies_csv)
        print(f"✓ 총 {len(self.companies_df)}개 기업 로드")
        
        # corp_code 컬럼명 확인 및 표준화
        if 'corp_code' in self.companies_df.columns:
            # 이미 있으면 그대로 사용
            pass
        elif 'stock_code' in self.companies_df.columns:
            # stock_code를 corp_code로 변경
            print("  ℹ️ stock_code 컬럼을 발견, 이름 변경")
            self.companies_df['stock_code_original'] = self.companies_df['corp_code']
        
        self.base_path = Path('data/raw')
        self.base_path.mkdir(parents=True, exist_ok=True)
        
        # 진행 상황 파일
        self.progress_file = Path('data/progress.json')
        self.progress = self.load_progress()
        
        print(f"✓ 이전 진행: 완료 {len(self.progress['completed'])}개, 실패 {len(self.progress['failed'])}개")
    
    def load_progress(self) -> Dict:
        """진행 상황 로드"""
        if self.progress_file.exists():
            with open(self.progress_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {'completed': [], 'failed': []}
    
    def save_progress(self):
        """진행 상황 저장"""
        with open(self.progress_file, 'w', encoding='utf-8') as f:
            json.dump(self.progress, f, indent=2, ensure_ascii=False)
    
    def get_corp_code(self, corp_name: str) -> Optional[str]:
        """
        회사명으로 DART 고유번호 조회 (API 직접 호출)
        """
        try:
            url = f"{BASE_URL}/corpCode.xml"
            params = {'crtfc_key': API_KEY}
            
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code != 200:
                logging.error(f"API 요청 실패: {response.status_code}")
                return None
            
            # ZIP 파일 압축 해제
            import zipfile
            import io
            
            zip_file = zipfile.ZipFile(io.BytesIO(response.content))
            xml_data = zip_file.read('CORPCODE.xml')
            
            # XML 파싱
            root = ET.fromstring(xml_data)
            
            # 회사명으로 검색
            for corp in root.findall('list'):
                name = corp.find('corp_name').text
                if name and corp_name in name:
                    corp_code = corp.find('corp_code').text
                    stock_code = corp.find('stock_code').text
                    logging.info(f"  찾음: {name} (고유번호: {corp_code}, 종목: {stock_code})")
                    return corp_code
            
            logging.warning(f"기업 '{corp_name}' 조회 결과 없음")
            return None
            
        except Exception as e:
            logging.error(f"기업 '{corp_name}' 조회 오류: {e}")
            return None
    
    def get_corp_info(self, corp_code: str, corp_name: str) -> Optional[Dict]:
        """기업 개황 정보 조회"""
        try:
            url = f"{BASE_URL}/company.json"
            params = {
                'crtfc_key': API_KEY,
                'corp_code': corp_code
            }
            
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code != 200:
                logging.error(f"{corp_name} 개황 조회 실패: HTTP {response.status_code}")
                return None
            
            data = response.json()
            
            # 에러 체크
            if data.get('status') != '000':
                logging.error(f"{corp_name} 개황 조회 실패: {data.get('message')}")
                return None
            
            return data
            
        except Exception as e:
            logging.error(f"{corp_name} 개황 조회 오류: {e}")
            return None
    
    def collect_corp_info(self, corp_code: str, corp_name: str):
        """기업 개황 정보 수집 및 저장"""
        try:
            info = self.get_corp_info(corp_code, corp_name)
            
            if not info:
                return False
            
            # 저장
            save_path = self.base_path / 'corp_info' / f'{corp_code}.json'
            save_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(save_path, 'w', encoding='utf-8') as f:
                json.dump(info, f, ensure_ascii=False, indent=2)
            
            logging.info(f"✓ {corp_name} 개황 저장 완료")
            return True
            
        except Exception as e:
            logging.error(f"{corp_name} 개황 수집 오류: {e}")
            return False
    
    def get_filings_list(self, corp_code: str, bgn_de: str, end_de: str) -> List[Dict]:
        """공시 목록 조회 (전체)"""
        try:
            url = f"{BASE_URL}/list.json"
            params = {
                'crtfc_key': API_KEY,
                'corp_code': corp_code,
                'bgn_de': bgn_de,
                'end_de': end_de,
                'page_count': 100
            }
            
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code != 200:
                return []
            
            data = response.json()
            
            if data.get('status') != '000':
                return []
            
            return data.get('list', [])
            
        except Exception as e:
            logging.error(f"공시 목록 조회 오류: {e}")
            return []
    
    def download_filing(self, rcept_no: str, save_path: Path):
        """공시 문서 다운로드"""
        try:
            url = f"{BASE_URL}/document.xml"
            params = {
                'crtfc_key': API_KEY,
                'rcept_no': rcept_no
            }
            
            response = requests.get(url, params=params, timeout=60)
            
            if response.status_code != 200:
                return False
            
            save_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(save_path, 'wb') as f:
                f.write(response.content)
            
            return True
            
        except Exception as e:
            logging.error(f"문서 다운로드 오류: {e}")
            return False
    
    def collect_filings(self, corp_code: str, corp_name: str, 
                       years: List[str] = ['2022', '2023', '2024']):
        """정기공시 문서 수집"""
        results = []
        
        for year in years:
            try:
                # API 호출 제한 준수
                time.sleep(0.5)
                
                # 해당 연도의 모든 공시 조회
                all_filings = self.get_filings_list(
                    corp_code=corp_code,
                    bgn_de=f'{year}0101',
                    end_de=f'{year}1231'
                )
                
                if not all_filings:
                    logging.info(f"  {corp_name} {year}: 공시 없음")
                    continue
                
                # 보고서 타입별로 필터링
                for report_type, keywords in REPORT_TYPES.items():
                    # 키워드에 매칭되는 공시 필터링
                    matched_filings = [
                        f for f in all_filings 
                        if any(keyword in f.get('report_nm', '') for keyword in keywords)
                    ]
                    
                    if not matched_filings:
                        logging.info(f"  {corp_name} {year} {report_type}: 없음")
                        continue
                    
                    logging.info(f"  {corp_name} {year} {report_type}: {len(matched_filings)}건 발견")
                    
                    # 각 공시 문서 다운로드
                    for filing in matched_filings:
                        rcept_no = filing['rcept_no']
                        report_nm = filing['report_nm']
                        
                        save_dir = self.base_path / 'filings' / corp_code / year
                        # 파일명을 보고서명으로 생성 (안전한 파일명으로 변환)
                        safe_report_nm = report_nm.replace('/', '_').replace('\\', '_')
                        filename = f"{year}_{rcept_no}_{safe_report_nm[:30]}.xml"
                        save_path = save_dir / filename
                        
                        # 이미 다운로드했으면 스킵
                        if save_path.exists():
                            logging.info(f"    이미 존재: {filename}")
                            continue
                        
                        # 원문 다운로드
                        time.sleep(0.5)
                        
                        if self.download_filing(rcept_no, save_path):
                            results.append({
                                'corp_name': corp_name,
                                'year': year,
                                'report_type': report_type,
                                'report_nm': report_nm,
                                'rcept_no': rcept_no,
                                'path': str(save_path)
                            })
                            logging.info(f"    ✓ {report_nm} 저장")
                        else:
                            logging.warning(f"    ✗ {report_nm} 다운로드 실패")
                
            except Exception as e:
                logging.error(f"  {corp_name} {year} 오류: {e}")
                continue
        
        return results
    
    def collect_all(self):
        """전체 기업 데이터 수집"""
        total = len(self.companies_df)
        
        print(f"\n{'='*60}")
        print(f"수집 시작: 총 {total}개 기업")
        print(f"{'='*60}\n")
        
        for idx, row in self.companies_df.iterrows():
            corp_name = row['corp_name']
            
            # 이미 완료된 기업은 스킵
            if corp_name in self.progress['completed']:
                print(f"[{idx+1}/{total}] {corp_name} - SKIP (이미 완료)")
                continue
            
            print(f"\n{'='*60}")
            print(f"[{idx+1}/{total}] {corp_name} 처리 시작")
            print(f"{'='*60}")
            
            try:
                # DART 고유번호 조회
                corp_code = self.get_corp_code(corp_name)
                if not corp_code:
                    self.progress['failed'].append({
                        'corp_name': corp_name,
                        'reason': '고유번호 조회 실패'
                    })
                    self.save_progress()
                    continue
                
                print(f"  ✓ DART 고유번호: {corp_code}")
                
                # 기업 개황 수집
                if not self.collect_corp_info(corp_code, corp_name):
                    self.progress['failed'].append({
                        'corp_name': corp_name,
                        'reason': '개황 수집 실패'
                    })
                    self.save_progress()
                    continue
                
                # 정기공시 수집
                print(f"  📄 정기공시 수집 중...")
                filings = self.collect_filings(corp_code, corp_name)
                
                # 성공 기록
                self.progress['completed'].append(corp_name)
                self.save_progress()
                
                print(f"✓✓✓ {corp_name} 완료 ({len(filings)}개 문서)")
                
            except KeyboardInterrupt:
                print("\n\n⚠️ 사용자가 중단했습니다")
                self.save_progress()
                print(f"진행 상황 저장 완료: {len(self.progress['completed'])}개 완료")
                return
                
            except Exception as e:
                logging.error(f"✗✗✗ {corp_name} 실패: {e}")
                self.progress['failed'].append({
                    'corp_name': corp_name,
                    'reason': str(e)
                })
                self.save_progress()
                continue
            
            # API 과부하 방지
            time.sleep(1)
        
        print(f"\n{'='*60}")
        print(f"🎉 수집 완료!")
        print(f"{'='*60}")
        print(f"✓ 성공: {len(self.progress['completed'])}개")
        print(f"✗ 실패: {len(self.progress['failed'])}개")
        
        if self.progress['failed']:
            print(f"\n실패한 기업 목록:")
            for item in self.progress['failed'][:10]:
                print(f"  - {item['corp_name']}: {item['reason']}")
            if len(self.progress['failed']) > 10:
                print(f"  ... 외 {len(self.progress['failed']) - 10}개")
        
        print(f"{'='*60}\n")

if __name__ == '__main__':
    try:
        collector = DartCollector()
        collector.collect_all()
    except Exception as e:
        print(f"\n❌ 치명적 오류: {e}")
        import traceback
        traceback.print_exc()