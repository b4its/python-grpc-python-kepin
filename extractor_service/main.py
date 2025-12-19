import grpc
from concurrent import futures
import pandas as pd
import io
import json
import re
import logging 
import warnings
from dotenv import load_dotenv

import financial_pb2
import financial_pb2_grpc

# Abaikan warning openpyxl
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

# --- KONFIGURASI LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("financial_engine.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()

class UltimateFinancialScanner:
    def __init__(self):
        self.targets = {
            "total_aset": [
                re.compile(r"^jumlah\s*aset(?!.*lancar|.*tidak lancar|.*pajak|.*tetap)", re.IGNORECASE), 
                re.compile(r"^total\s*assets(?!.*current|.*non-current|.*tax|.*fixed)", re.IGNORECASE),
                re.compile(r"^jumlah\s*aktiva(?!.*lancar)", re.IGNORECASE),
            ],
            "total_liabilitas": [
                re.compile(r"^jumlah\s*liabilitas(?!.*lancar|.*jangka|.*ekuitas|.*equity|.*neto)", re.IGNORECASE), 
                re.compile(r"^total\s*liabilities(?!.*current|.*term|.*equity|.*net)", re.IGNORECASE),
                re.compile(r"^jumlah\s*kewajiban(?!.*lancar|.*jangka|.*ekuitas)", re.IGNORECASE)
            ],
            "total_ekuitas": [
                re.compile(r"^jumlah\s*ekuitas(?!.*liabilitas|.*kewajiban)", re.IGNORECASE), 
                re.compile(r"^total\s*equity(?!.*liabilities)", re.IGNORECASE),
                re.compile(r"^jumlah\s*ekuitas\s*yang\s*diatribusikan.*pemilik.*entitas.*induk", re.IGNORECASE) 
            ],
            "laba_bersih": [
                re.compile(r"laba.*(rugi)?.*yang.*dapat.*diatribusikan.*ke.*entitas.*induk", re.IGNORECASE),
                re.compile(r"profit.*(loss)?.*attributable.*to.*parent.*entity", re.IGNORECASE),
                re.compile(r"^laba.*(rugi)?.*tahun.*berjalan.*atribusikan.*kepada.*entitas.*induk", re.IGNORECASE),
                re.compile(r"^laba.*(rugi)?.*tahun.*berjalan$", re.IGNORECASE),
                re.compile(r"^profit.*(loss)?.*for.*the.*year$", re.IGNORECASE)
            ]
        }
        
        self.scale_map = {
            "jutaan": 1_000_000,
            "millions": 1_000_000,
            "ribuan": 1_000,
            "thousands": 1_000,
            "miliar": 1_000_000_000,
            "billions": 1_000_000_000
        }

    def clean_numeric(self, val):
        if isinstance(val, (int, float)):
            return float(val) if not pd.isna(val) else None
            
        if pd.isna(val) or val == "" or str(val).strip() in ["-", "n/a", "", "nan"]: return None
        
        s = str(val).strip()
        if not s: return None

        if s.startswith('(') and s.endswith(')'):
            s = '-' + s[1:-1]
        try:
            clean_s = re.sub(r'[^\d\.\-]', '', s)
            if not clean_s: return None
            return float(clean_s)
        except:
            return None

    def detect_scale_and_currency(self, df):
        rows_to_check = 20
        # Safety: pastikan kolom ada sebelum akses iloc
        if df.shape[1] == 0: return 1.0, "IDR", "Full Amount"

        header_col1 = df.iloc[:rows_to_check, 0].fillna("").astype(str).str.lower().tolist()
        header_rows = header_col1
        
        if df.shape[1] > 1:
            header_col2 = df.iloc[:rows_to_check, 1].fillna("").astype(str).str.lower().tolist()
            header_rows += header_col2

        full_text = " ".join(header_rows)
        
        multiplier = 1.0
        scale_label = "Full Amount"
        currency = "IDR" 
        
        if "usd" in full_text or "dollar" in full_text or "dolar" in full_text or "as$" in full_text:
            currency = "USD"
        
        for key, val in self.scale_map.items():
            if key in full_text:
                multiplier = float(val)
                scale_label = key.title()
                break 
        
        return multiplier, currency, scale_label

    def find_best_value_column(self, df):
        num_rows, num_cols = df.shape
        # FIX 1: Jika kolom < 2, return 0 (jangan 1 karena 1 itu out of bounds)
        if num_cols < 2: return 0 

        check_limit = min(15, num_rows)
        subset = df.iloc[:check_limit, :].fillna("").astype(str) 

        for r in range(check_limit):
            row_vals = [x.lower() for x in subset.iloc[r, :]]
            for c, val in enumerate(row_vals):
                if "current" in val or "2024" in val or "2025" in val or "berjalan" in val:
                    if r+3 < num_rows:
                         check_val = self.clean_numeric(df.iloc[r+3, c])
                         if check_val is not None: return c
        
        max_numeric_count = 0
        best_col = 1 # Default ideal
        
        # Safety: Pastikan range tidak melebihi num_cols
        scan_end = min(7, num_cols)
        
        scan_rows = min(100, num_rows)
        for c in range(1, scan_end): 
            col_slice = df.iloc[:scan_rows, c]
            numeric_count = pd.to_numeric(col_slice, errors='coerce').notna().sum()
            
            if numeric_count > max_numeric_count:
                max_numeric_count = numeric_count
                best_col = c
        
        # Safety final: jika loop di atas tidak jalan (misal num_cols=1), return 0
        if best_col >= num_cols:
            return 0
            
        return best_col

    def scan_all(self, dfs):
        res = {
            "nama_entitas": "Unknown",
            "periode_laporan": "2024-12-31",
            "mata_uang": "IDR",
            "satuan_angka": "Full Amount",
            "total_aset": 0.0,
            "total_liabilitas": 0.0,
            "total_ekuitas": 0.0,
            "laba_bersih": 0.0,
            "data_keuangan_lain": []
        }

        other_keywords = ["kas dan setara kas", "persediaan", "pendapatan", "beban pokok", "laba bruto", "penjualan", "laba usaha", "laba sebelum pajak"]
        global_currency = "IDR"
        
        for name, df in dfs.items():
            if df.empty: continue
            
            sheet_multiplier, sheet_currency, sheet_scale_label = self.detect_scale_and_currency(df)
            
            if sheet_currency == "USD": 
                global_currency = "USD"
                res["mata_uang"] = "USD"
            
            if sheet_multiplier != 1.0:
                res["satuan_angka"] = sheet_scale_label

            target_col = self.find_best_value_column(df)
            
            # FIX 2: Double Safety Check. Jika target_col di luar batas, paksa ke kolom terakhir
            if target_col >= df.shape[1]:
                target_col = max(0, df.shape[1] - 1)
                
            num_rows = df.shape[0]

            # Jika sheet kosong (0 kolom), skip
            if df.shape[1] == 0: continue

            # Ambil kolom label (Col 0)
            labels_col0 = df.iloc[:, 0].fillna("").astype(str).str.lower().str.strip().values
            
            labels_col1 = None
            if df.shape[1] > 1:
                labels_col1 = df.iloc[:, 1].fillna("").astype(str).str.lower().str.strip().values
            
            # Ambil kolom value (Safe now)
            values_col = df.iloc[:, target_col].tolist()

            for r in range(num_rows):
                label_clean = labels_col0[r]
                
                if (label_clean == "nan" or label_clean == "") and labels_col1 is not None:
                    label_clean = labels_col1[r]
                
                if not label_clean: continue

                if '  ' in label_clean:
                    label_clean = re.sub(r'\s+', ' ', label_clean)

                if "nama entitas" in label_clean and res["nama_entitas"] == "Unknown":
                    # Cek bounds untuk col 1
                    if df.shape[1] > 1:
                        res["nama_entitas"] = str(df.iloc[r, 1]).strip()
                elif "tanggal akhir" in label_clean:
                    if df.shape[1] > 1:
                        res["periode_laporan"] = str(df.iloc[r, 1]).strip()

                current_val = None 
                
                for key, patterns in self.targets.items():
                    matched = False
                    for pattern in patterns:
                        if pattern.search(label_clean):
                            if current_val is None:
                                current_val = self.clean_numeric(values_col[r])
                            
                            val = current_val
                            
                            if val is not None:
                                final_val = val * sheet_multiplier
                                
                                if key == "laba_bersih" and res["total_aset"] > 0:
                                     if abs(final_val) > res["total_aset"]: continue
                                
                                if abs(final_val) > abs(res[key]):
                                    res[key] = final_val
                                matched = True
                                break 
                    if matched: break 

                if any(okw in label_clean for okw in other_keywords):
                    if current_val is None:
                        current_val = self.clean_numeric(values_col[r])
                    
                    val = current_val
                    if val is not None and val != 0:
                        if not any(d['keterangan'] == label_clean for d in res["data_keuangan_lain"]):
                            res["data_keuangan_lain"].append({
                                "keterangan": label_clean,
                                "nilai": val * sheet_multiplier
                            })
                            
        res["mata_uang"] = global_currency
        return res

class FinancialExtractorServicer(financial_pb2_grpc.FinancialExtractorServicer):
    def ExtractAndAnalyze(self, request, context):
        print(f"\n📥 [REQUEST] Menerima file: {request.file_name}")
        yield financial_pb2.AnalyzeResponse(log_message="ENGINE: Menjalankan Pemindaian Presisi (Optimized)...")

        try:
            excel_file = io.BytesIO(request.file_content)
            
            # Tips Performance: pd.read_excel adalah bottleneck terbesar.
            # Menggunakan engine 'openpyxl' (default).
            dfs = pd.read_excel(excel_file, sheet_name=None, header=None)
            
            scanner = UltimateFinancialScanner()
            data = scanner.scan_all(dfs)

            if data["nama_entitas"] == "Unknown":
                data["nama_entitas"] = request.file_name.split('.')[0]

            json_string = json.dumps(data["data_keuangan_lain"]) 
            
            print(f"[RESULT] Entitas: {data['nama_entitas']}")
            print(f"[RESULT] Mata Uang: {data['mata_uang']} | Satuan: {data['satuan_angka']}")
            print(f"[RESULT] Aset: {data['total_aset']:,.2f}")
            print(f"[RESULT] Liabilitas: {data['total_liabilitas']:,.2f}")
            print(f"[RESULT] Laba: {data['laba_bersih']:,.2f}")

            final_res = financial_pb2.FinancialResult(
                nama_entitas=str(data["nama_entitas"]),
                periode_laporan=str(data["periode_laporan"]),
                mata_uang=str(data["mata_uang"]),
                satuan_angka=str(data["satuan_angka"]),
                total_aset=float(data["total_aset"]),
                total_liabilitas=float(data["total_liabilitas"]),
                total_ekuitas=float(data["total_ekuitas"]),
                laba_bersih=float(data["laba_bersih"]),
                json_data_lain=json_string
            )

            yield financial_pb2.AnalyzeResponse(final_data=final_res)
            
            print("\n[FULL JSON OUTPUT]:")
            print(json.dumps(data, indent=2)) 

        except Exception as e:
            logger.error(f"Error: {str(e)}", exc_info=True)
            yield financial_pb2.AnalyzeResponse(error_message=f"Error: {str(e)}")

def serve():
    # Menambah worker sedikit jika I/O bound, tapi 10 sudah cukup oke untuk file excel
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    financial_pb2_grpc.add_FinancialExtractorServicer_to_server(FinancialExtractorServicer(), server)
    server.add_insecure_port('[::]:50051')
    print("🚀 IDX-Optimized Financial Engine (v6.1 High-Performance) running on port 50051")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()