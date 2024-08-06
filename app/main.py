from app.modules.read_data import DataProcessor
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
import os
import tempfile
import polars as pl

app = FastAPI()

schema = {
    'カンパニー名': pl.Utf8,
    'カンパニーコード': pl.Utf8,
    '店舗名': pl.Utf8,
    '店舗コード': pl.Utf8,
    '注文ID': pl.Utf8,
    '注文日': pl.Utf8,
    '配送指定日': pl.Utf8,
    '配送エリア名': pl.Utf8,
    '配送エリアコード': pl.Utf8,
    '配送会社': pl.Utf8,
    '配送種別': pl.Utf8,
    '出荷ステータス': pl.Utf8,
    '配送日': pl.Utf8,
    'キャンセル日': pl.Utf8,
    '返品日': pl.Utf8,
    '支払種別': pl.Utf8,
    '商品金額合計_税抜': pl.Float64,
    '送料_税抜': pl.Float64,
    '手数料_税抜': pl.Float64,
    'ポイント利用値引き': pl.Float64,
    '消費税': pl.Float64,
    '注文金額合計_税込': pl.Float64,
    '連絡欄コメント': pl.Utf8,
}

data_processor = DataProcessor(schema)

app.mount("/static", StaticFiles(directory="app/static", html=True), name="static")

@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.get("/items/{item_id}")
def read_item(item_id: int, q: str = None):
    return {"item_id": item_id, "q": q}
  
@app.post("/process_xlsx/")
async def process_xlsx(file: UploadFile = File(...)):
  if not file.filename.endswith('.xlsx'):
    raise HTTPException(status_code=400, detail="excel形式でアップしてください")
  
  with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as temp_file:
    temp_file_path = temp_file.name
    content = await file.read()
    temp_file.write(content)
    
  try:
    df = data_processor.load_data(temp_file_path)
    df_all_store, df_all_company = data_processor.process_data(df)
    output_file_path = data_processor.save_to_excel(df_all_store, df_all_company)

  except Exception as e:
    raise HTTPException(status_code=500, detail=str(e))
  finally:
    if os.path.exists(temp_file_path):
      os.remove(temp_file_path)
    
  return FileResponse(
    output_file_path,
    filename="置き楽状況元データ.xlsx"
  )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)