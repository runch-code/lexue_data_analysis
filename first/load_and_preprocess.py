import pandas as pd
import json
import chinese_province_city_area_mapper.mappers as mapper
import glob
import warnings
from tqdm import tqdm

def parse_purchase_history(record):
    """解析JSON格式的购买记录"""
    try:
        data = json.loads(record.replace("'", '"'))
        return pd.Series({
            'avg_price': data.get('average_price', 0),
            'category': data.get('category', 'unknown'),
            'items_count': len(data.get('items', []))
        })
    except:
        return pd.Series({'avg_price': 0, 'category': 'unknown', 'items_count': 0})

def load_data(file_pattern):
    """高效加载合并多个CSV文件"""
    chunks = []
    for file in glob.glob(file_pattern):
        for chunk in pd.read_csv(file, chunksize=10000):
            # 解析关键字段
            chunk[['avg_price', 'category', 'items_count']] = chunk['purchase_history'].apply(parse_purchase_history)
            
            # 地址解析
            # 从中文地址中提取省市信息
            # 从mapper.province_country_mapper的key中获取省信息列表（包含市，省，自治区）
            province_mapper = mapper.province_country_mapper
            province_list = list(province_mapper.keys())
            # 筛选只以市，省，自治区，特别行政区结尾的省信息
            province_list = [i for i in province_list if i.endswith(('市', '省', '自治区', '特别行政区'))]
            # 解析chunk["chinese_address"]列
            chunk['province'] = chunk['chinese_address'].apply(lambda x: [i for i in province_list if i in x])
            chunk['province'] = chunk['province'].apply(lambda x: x[0] if len(x) > 0 else 'unknown') # 如果没有匹配到省份，则标记为'unknown'
            # print(chunk['province'])
            
            chunks.append(chunk)
    return pd.concat(chunks)

def load_data_test(file_pattern):
    """高效加载合并多个CSV文件"""
    chunks = []
    
    # 获取文件列表
    files = glob.glob(file_pattern)
    
    # 第一层进度条：文件处理进度
    with tqdm(files, desc="📂 Processing files", unit="file") as file_pbar:
        for file in file_pbar:
            # 设置当前文件的进度描述
            file_pbar.set_postfix(file=file.split("\\")[-1][:10])  # 显示文件名（取后10字符）
            
            # 第二层进度条：块处理进度
            chunk_iter = pd.read_csv(file, chunksize=1000000)
            with tqdm(desc="📦 Processing chunks", unit="chunk", leave=False) as chunk_pbar:
                for chunk in chunk_iter:
                    # 解析关键字段
                    chunk[['avg_price', 'category', 'items_count']] = chunk['purchase_history'].apply(parse_purchase_history)
                    
                    # 地址解析
                    # 从中文地址中提取省市信息
                    province_mapper = mapper.province_country_mapper
                    province_list = [p for p in province_mapper.keys() 
                                   if p.endswith(('市', '省', '自治区', '特别行政区'))]
                    # 解析chunk["chinese_address"]列
                    chunk['province'] = chunk['chinese_address'].apply(
                        lambda x: next((p for p in province_list if p in x), 'unknown'))
                    
                    chunks.append(chunk)
                    chunk_pbar.update(1)  # 更新块进度条
                    chunk_pbar.set_postfix(current_size=f"{len(chunks)*10000:,} rows") 
    return pd.concat(chunks)
    
def main():
    # 使用示例
    df = load_data("./data/*.csv")  # 支持通配符匹配多个文件
    
if __name__ == "__main__":
    main()