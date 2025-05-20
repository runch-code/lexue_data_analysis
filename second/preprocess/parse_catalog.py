import json

def load_product_catalog(path):
    with open(path, 'r', encoding='utf-8') as f:
        catalog = json.load(f)
    # 映射字典: {id: category}
    id_to_category = {item['id']: item['category'] for item in catalog['products']}
    id_to_price = {item['id']: item['price'] for item in catalog['products']}
    return id_to_category, id_to_price

def extract_categories(x, id2cat):
    # print(x)
    try:
        data = json.loads(x.replace("'", '"'))  # 替换单引号防止格式错误
        # print(data.get('items', []))
        return [id2cat.get(item.get('id'), '未知') 
                for item in data.get('items', []) if isinstance(item, dict)]
    except Exception:
        return []

def extract_prices(x):
    try:
        data = json.loads(x.replace("'", '"'))
        return [item.get('price', 0) 
                for item in data.get('items', []) if isinstance(item, dict)]
    except Exception:
        return []