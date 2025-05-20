# utils/metrics.py
def calculate_lift(confidence, support_rhs):
    """计算提升度"""
    return confidence / support_rhs if support_rhs != 0 else 0

def calculate_all_metrics(freq_itemsets, rules):
    """补充提升度、覆盖度等指标"""
    rules = rules.copy()
    rules['support_rhs'] = rules['consequents'].apply(
        lambda rhs: freq_itemsets[freq_itemsets['itemsets'] == rhs]['support'].values[0] if not freq_itemsets[freq_itemsets['itemsets'] == rhs].empty else 0)
    rules['lift'] = rules.apply(lambda row: calculate_lift(row['confidence'], row['support_rhs']), axis=1)
    return rules
