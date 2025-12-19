import grpc
from concurrent import futures
import time
import pandas as pd
import io
import json
import http.client
import os
from dotenv import load_dotenv

# Load environment variables dari file .env
load_dotenv()

# Import hasil generate proto
import financial_pb2
import financial_pb2_grpc

class FinancialExtractorServicer(financial_pb2_grpc.FinancialExtractorServicer):
    def ExtractAndAnalyze(self, request, context):
        filename = request.file_name
        mode = getattr(request, 'analyze_mode', 'normal')
        
        # Ambil API Key dari Environment Variable
        api_token = os.getenv("KOLOSAL_API_KEY")
        
        print(f"[*] [{mode.upper()}] Memproses file: {filename}")

        # 1. Kirim progress awal ke Rust -> Frontend
        yield financial_pb2.AnalyzeResponse(log_message=f"START: Memulai analisis {mode} untuk {filename}...")

        try:
            if not api_token:
                raise ValueError("Environment variable KOLOSAL_API_KEY tidak ditemukan!")

            # --- TAHAP 1: EKSTRAKSI EXCEL KE CSV ---
            content_str = ""
            if request.file_extension.lower() in ['xlsx', 'xls']:
                yield financial_pb2.AnalyzeResponse(log_message="PARSING: Membaca tabel dari semua sheet Excel...")
                
                excel_file = io.BytesIO(request.file_content)
                # Gunakan engine openpyxl untuk file xlsx
                with pd.ExcelFile(excel_file) as xls:
                    sheets_content = []
                    for sheet_name in xls.sheet_names:
                        df = pd.read_excel(xls, sheet_name=sheet_name)
                        # Hapus baris/kolom yang kosong total
                        df = df.dropna(how='all').dropna(axis=1, how='all')
                        
                        if not df.empty:
                            csv_data = df.to_csv(index=False)
                            sheets_content.append(f"--- SHEET: {sheet_name} ---\n{csv_data}")
                    
                    content_str = "\n\n".join(sheets_content)
            else:
                # Fallback jika file teks biasa/CSV
                content_str = request.file_content.decode('utf-8', errors='ignore')

            # Batasi panjang teks agar tidak melebihi context window AI (sekitar 30k karakter)
            if len(content_str) > 40000:
                content_str = content_str[:40000] + "... [DATA TRUNCATED]"
                yield financial_pb2.AnalyzeResponse(log_message="INFO: Data terlalu besar, melakukan pemotongan teks...")

            # --- TAHAP 2: INTEGRASI KOLOSAL AI ---
            yield financial_pb2.AnalyzeResponse(log_message="AI: Mengirim data finansial ke Kolosal AI...")

            system_prompt = (
                "You are a Professional Financial Statement Extractor. "
                "Your task is to extract key figures from the provided CSV data. "
                "Output MUST be a valid JSON object only. "
                "Keys: nama_entitas, periode_laporan (YYYY-MM-DD), mata_uang, satuan_angka, "
                "total_aset, total_liabilitas, total_ekuitas, laba_bersih, "
                "data_keuangan_lain (array of objects {keterangan: string, nilai: number})."
            )
            
            user_prompt = f"Analyze this data and extract into JSON:\n\n{content_str}"

            # Setup HTTP Connection ke Kolosal
            conn = http.client.HTTPSConnection("api.kolosal.ai")
            payload = json.dumps({
                "max_tokens": 2000,
                "messages": [
                    {"content": system_prompt, "role": "system"},
                    {"content": user_prompt, "role": "user"}
                ],
                "model": "meta-llama/llama-4-maverick-17b-128e-instruct"
            })

            headers = {
                'Content-Type': "application/json",
                # FIX: Tambahkan kata 'Bearer ' di depan token jika belum ada
                'Authorization': f"Bearer {api_token}" if not api_token.startswith("Bearer ") else api_token
            }

            conn.request("POST", "/v1/chat/completions", payload, headers)
            res = conn.getresponse()
            raw_response = res.read().decode("utf-8")
            
            # Parsing response JSON dari API
            api_json = json.loads(raw_response)
            
            if 'choices' not in api_json:
                raise ValueError(f"AI API Error: {raw_response}")

            content_res = api_json['choices'][0]['message']['content']

            # Pembersihan Markdown jika AI membungkus JSON dengan ```json
            if "```json" in content_res:
                content_res = content_res.split("```json")[1].split("```")[0].strip()
            elif "```" in content_res:
                content_res = content_res.split("```")[1].split("```")[0].strip()

            final_data_json = json.loads(content_res)

            # --- TAHAP 3: KIRIM HASIL KE RUST ---
            yield financial_pb2.AnalyzeResponse(log_message="DONE: Analisis AI selesai, mengirim data ke database...")

            final_res = financial_pb2.FinancialResult(
                nama_entitas=str(final_data_json.get("nama_entitas", "Unknown")),
                periode_laporan=str(final_data_json.get("periode_laporan", "")),
                mata_uang=str(final_data_json.get("mata_uang", "IDR")),
                satuan_angka=str(final_data_json.get("satuan_angka", "Penuh")),
                total_aset=float(final_data_json.get("total_aset", 0)),
                total_liabilitas=float(final_data_json.get("total_liabilitas", 0)),
                total_ekuitas=float(final_data_json.get("total_ekuitas", 0)),
                laba_bersih=float(final_data_json.get("laba_bersih", 0)),
                json_data_lain=json.dumps(final_data_json.get("data_keuangan_lain", []))
            )

            yield financial_pb2.AnalyzeResponse(final_data=final_res)

        except Exception as e:
            print(f"[!] Error: {str(e)}")
            yield financial_pb2.AnalyzeResponse(error_message=f"Python Service Error: {str(e)}")

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    financial_pb2_grpc.add_FinancialExtractorServicer_to_server(FinancialExtractorServicer(), server)
    server.add_insecure_port('[::]:50051')
    print("🚀 Python gRPC Server running on port 50051")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()