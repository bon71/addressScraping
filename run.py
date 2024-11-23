from address_validator import MedicalAddressValidator
import logging
from datetime import datetime

def main():
    # 実行時のタイムスタンプを生成
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # 入出力ファイルのパスを設定
    input_file = 'data/input/medical_facilities.csv'
    output_file = f'data/output/updated_addresses_{timestamp}.csv'

    try:
        # バリデーターの初期化
        validator = MedicalAddressValidator(batch_size=50)

        print("処理を開始します...")
        results = validator.process_all_data(input_file, output_file)

        # 結果の概要を表示
        total_records = len(results)
        updated_records = results['new_postal_code'].notna().sum()

        print("\n処理結果サマリー:")
        print(f"総レコード数: {total_records}")
        print(f"更新済みレコード数: {updated_records}")
        print(f"未更新レコード数: {total_records - updated_records}")
        print(f"\n結果は {output_file} に保存されました。")

    except Exception as e:
        print(f"エラーが発生しました: {str(e)}")
        logging.error(f"Error in main execution: {str(e)}")

if __name__ == "__main__":
    main()
