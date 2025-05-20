import pandas as pd
import json
import glob
from tqdm import tqdm
from preprocess.load_and_preprocess import load_parquet_data
from preprocess.parse_catalog import load_product_catalog

def extract_items_from_history(row, id_to_category, id_to_price):
    """提取购买的商品类别和价格"""
    try:
        data = json.loads(row.replace("'", '"'))
        items = data.get("items", [])
        categories = [id_to_category.get(item["item_id"], "未知") for item in items]
        prices = [item.get("price", id_to_price.get(item["item_id"], 0)) for item in items]
        return pd.Series([categories, prices])
    except Exception:
        return pd.Series([[], []])

def preprocess_parquet_data(parquet_glob_path, catalog_path):
    """加载并预处理多个 Parquet 文件"""
    print(f"开始预处理：{parquet_glob_path}")
    
    # 加载商品目录
    id_to_category, id_to_price = load_product_catalog(catalog_path)
    
    # 加载Parquet数据
    df = load_parquet_data(glob.glob(parquet_glob_path), if_file_pattern=False)
    
    # 解析 purchase_history 字段
    tqdm.pandas(desc="解析 purchase_history")
    df[['categories', 'prices']] = df['purchase_history'].progress_apply(
        lambda x: extract_items_from_history(x, id_to_category, id_to_price)
    )
    
    return df

if __name__ == "__main__":
    # 示例调用（支持通配符）
    processed_df = preprocess_parquet_data("data/*.parquet", "data/product_catalog_test.json")
    processed_df.to_pickle("data/processed.pkl")  # 后续分析使用
    print("预处理完成，数据已保存为 data/processed.pkl")
