import polars as pl
import pandas as pd
import tempfile

class DataProcessor:
  def __init__(self, schema):
    self.schema = schema

  def load_data(self, file_path: str):
    df = pl.read_excel(file_path, schema_overrides=self.schema)
    return df

  def process_data(self, df: pl.DataFrame):
    # Clean and process data
    df_cleaned = (
      df
      .with_columns(
        pl.when(pl.col('配送種別') == "非対面受渡し")
        .then(1)
        .otherwise(0)
        .alias('非対面'),
        pl.when(
          (pl.col('連絡欄コメント').str.contains('非対面')) |
          (pl.col('連絡欄コメント').str.contains('非接触')) |
          (pl.col('連絡欄コメント').str.contains('非対応'))
        )
        .then(1)
        .otherwise(0)
        .alias('非対面コメント')
      )
      .filter(pl.col('出荷ステータス') == '完了')
      .select([
        'カンパニー名',
        'カンパニーコード',
        '店舗名',
        '店舗コード',
        '注文金額合計_税込',
        '非対面',
        '非対面コメント',
      ])
    )

    df_non_face = (
        df_cleaned
        .filter(
          (pl.col('非対面') == 1) |
          (pl.col('非対面コメント') == 1)
        )
        .drop(['非対面', '非対面コメント'])
        .group_by([
          'カンパニー名',
          'カンパニーコード',
          '店舗名',
          '店舗コード',
        ])
        .agg(
          pl.col('注文金額合計_税込').count().alias('count_non_face'),
          pl.col('注文金額合計_税込').sum().alias('sum_non_face'),
        )
    )

    df_all = (
      df_cleaned
      .drop(['非対面', '非対面コメント'])
      .group_by([
        'カンパニー名',
        'カンパニーコード',
        '店舗名',
        '店舗コード',
      ])
      .agg(
        pl.col('注文金額合計_税込').count().alias('count_all'),
        pl.col('注文金額合計_税込').sum().alias('sum_all'),
      )
    )

    df_all_store = (
      df_non_face
      .join(
        df_all,
        how='full',
        coalesce=True,
        on=[
          'カンパニー名',
          'カンパニーコード',
          '店舗名',
          '店舗コード',
        ]
      )
      .fill_null(0)
      .sort(['カンパニーコード', '店舗コード'])
      .with_columns(
        (pl.col('count_non_face') / pl.col('count_all')).alias('件数構成比'),
        (pl.col('sum_non_face') / pl.col('sum_all')).alias('金額構成比'),
      )
    )

    df_all_company = (
      df_all_store
      .select([
        'カンパニー名',
        'カンパニーコード',
        'count_non_face',
        'sum_non_face',
        'count_all',
        'sum_all',
      ])
      .group_by(['カンパニー名', 'カンパニーコード'])
      .sum()
      .with_columns(
        (pl.col('count_non_face') / pl.col('count_all')).alias('件数構成比'),
        (pl.col('sum_non_face') / pl.col('sum_all')).alias('金額構成比'),
      )
      .sort('カンパニーコード')
    )
    
    df_all_store, df_all_company = (
      df_all_store.select([
        'カンパニー名',
        'カンパニーコード',
        '店舗名',
        '店舗コード',
        'count_non_face',
        'sum_non_face',
        'count_all',
        '件数構成比',
      ]),
      df_all_company.select([
        'カンパニー名',
        'カンパニーコード',
        'count_non_face',
        'sum_non_face',
        'count_all',
        '件数構成比',
      ])
    )

    return df_all_store, df_all_company

  def save_to_excel(self, df_all_store: pl.DataFrame, df_all_company: pl.DataFrame):
    # Create a temporary output file for the Excel
    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as output_file:
      output_file_path = output_file.name

      # Write the DataFrames to Excel using pandas
      with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
        df_all_store.to_pandas().to_excel(writer, index=False, sheet_name='Store Data')
        df_all_company.to_pandas().to_excel(writer, index=False, sheet_name='Company Data')

    return output_file_path
