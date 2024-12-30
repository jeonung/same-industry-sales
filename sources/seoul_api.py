import os

import pandas as pd
import requests


class SeoulOpenAPI:
    def __init__(self, auth_key, base_url):
        """
        서울시 Open API 클래스 초기화
        """
        self.auth_key = auth_key
        self.base_url = base_url

    def fetch_data(self, start, end):
        """
        start와 end를 사용해 데이터를 API에서 가져옵니다.
        """
        url = f"{self.base_url}/{start}/{end}/"
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()  # JSON 데이터를 반환
        else:
            print(f"Error: {response.status_code}")
            return None

    def get_total_count(self, response):
        """
        응답 데이터에서 전체 데이터 개수를 추출합니다.
        모든 API에 호환되도록 설계.
        """
        try:
            # 응답의 첫 번째 키 가져오기 (API마다 최상위 키가 다름)
            main_key = list(response.keys())[0]
            return int(response[main_key]["list_total_count"])
        except KeyError:
            print("데이터 구조에 'list_total_count'가 없습니다.")
            return None

    def get_all_data(self, page_size=1000):
        """
        데이터를 처음부터 끝까지 page_size 개씩 나누어 가져옵니다.
        """
        all_data = []  # 데이터를 누적할 리스트
        start = 1  # 데이터 시작 인덱스

        # 첫 번째 호출로 전체 데이터 개수를 파악
        response = self.fetch_data(start, start + page_size - 1)
        if response:
            total_data_count = self.get_total_count(response)
            if total_data_count is None:
                return None

            print(f"총 데이터 개수: {total_data_count}")

            # 응답의 첫 번째 키 가져오기
            main_key = list(response.keys())[0]

            all_data.extend(response[main_key]["row"])  # 첫 번째 호출 결과 추가
        else:
            print("API 호출 실패")
            return None

        # 반복적으로 데이터 호출
        for start in range(page_size + 1, total_data_count + 1, page_size):
            end = start + page_size - 1
            print(f"Fetching data from {start} to {end}...")
            response = self.fetch_data(start, end)
            if response:
                try:
                    all_data.extend(response[main_key]["row"])  # 데이터 추가
                except KeyError:
                    print(
                        f"데이터 구조 오류: {start}~{end} 범위에서 데이터가 없습니다."
                    )
            else:
                print(f"{start}~{end} 데이터 호출 실패")
                break

        return all_data

    def save_to_csv(self, data, filename):
        """
        데이터를 DataFrame으로 변환하여 ../data/raw_data 경로에 CSV로 저장합니다.
        저장 전에 STDR_YYQU_CD 기준으로 오름차순 정렬합니다.
        """
        if data:
            df = pd.DataFrame(data)
            # STDR_YYQU_CD 기준으로 오름차순 정렬
            if "STDR_YYQU_CD" in df.columns:
                df = df.sort_values(by="STDR_YYQU_CD", ascending=True)

            # 저장 경로 설정
            save_path = os.path.join("..", "data", "raw_data", filename)
            os.makedirs(os.path.dirname(save_path), exist_ok=True)  # 경로가 없으면 생성
            df.to_csv(save_path, index=False, encoding="utf-8-sig")
            print(f"데이터가 {save_path} 파일에 저장되었습니다!")
        else:
            print("저장할 데이터가 없습니다.")
