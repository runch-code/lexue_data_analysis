# analysis/association_analysis.py
import pandas as pd
from mlxtend.frequent_patterns import apriori, association_rules
from mlxtend.preprocessing import TransactionEncoder

def prepare_category_transactions(df):
    """按订单构建商品类别交易列表"""
    return df['categories'].dropna().tolist()

def run_apriori(transactions, min_support=0.02, min_confidence=0.5):
    te = TransactionEncoder()
    te_ary = te.fit(transactions).transform(transactions)
    df_encoded = pd.DataFrame(te_ary, columns=te.columns_)

    freq_itemsets = apriori(df_encoded, min_support=min_support, use_colnames=True)
    rules = association_rules(freq_itemsets, metric="confidence", min_threshold=min_confidence)
    rules['lift'] = rules['lift'].round(3)
    return rules

def filter_rules_by_category(rules, target_category="电子产品"):
    return rules[rules['antecedents'].astype(str).str.contains(target_category) |
                 rules['consequents'].astype(str).str.contains(target_category)]

def analyze_payment_category_rules(df, min_support=0.01, min_confidence=0.6):
    """分析支付方式与商品类别的关联"""
    records = []
    for _, row in df.iterrows():
        for category in row['categories']:
            records.append([row['payment_method'], category])
    te = TransactionEncoder()
    te_ary = te.fit(records).transform(records)
    df_encoded = pd.DataFrame(te_ary, columns=te.columns_)
    freq_itemsets = apriori(df_encoded, min_support=min_support, use_colnames=True)
    rules = association_rules(freq_itemsets, metric="confidence", min_threshold=min_confidence)
    return rules

def analyze_high_value_payment(df):
    """分析高价商品的支付方式分布（价格 > 5000）"""
    high_value = df.explode(['categories', 'prices'])
    return high_value[high_value['prices'] > 5000]['payment_method'].value_counts().reset_index().rename(columns={'index': 'payment_method', 'payment_method': 'count'})