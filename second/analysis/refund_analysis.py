# analysis/refund_analysis.py
import pandas as pd
from mlxtend.frequent_patterns import apriori, association_rules
from mlxtend.preprocessing import TransactionEncoder

def analyze_refund_combinations(df, min_support=0.005, min_confidence=0.4):
    refund_df = df[df['payment_status'].isin(["已退款", "部分退款"])]
    transactions = refund_df['categories'].dropna().tolist()
    te = TransactionEncoder()
    te_ary = te.fit(transactions).transform(transactions)
    df_encoded = pd.DataFrame(te_ary, columns=te.columns_)
    freq_itemsets = apriori(df_encoded, min_support=min_support, use_colnames=True)
    rules = association_rules(freq_itemsets, metric="confidence", min_threshold=min_confidence)
    return rules
