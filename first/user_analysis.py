import pandas as pd

def build_user_profiles_old(df):
    """构建用户画像标签体系"""
    # 基础标签
    df['is_high_income'] = df['income'] > df['income'].quantile(0.8)
    df['is_frequent_buyer'] = df.groupby('user_name')['timestamp'].transform('count') > 3
    
    # 首先确保将timestamp列转换为datetime类型
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # RFM模型计算
    snapshot_date = pd.to_datetime(df['timestamp']).max() + pd.Timedelta(days=1)
    print(snapshot_date)
    rfm = df.groupby('user_name').agg({
        'timestamp': lambda x: (snapshot_date - x.max()).days,
        'user_name': 'count',
        'avg_price': 'sum'
    }).rename(columns={
        'timestamp': 'recency',
        'user_name': 'frequency',
        'avg_price': 'monetary'
    })
    print(rfm)
    
    # 处理可能出现的重复分箱边界问题
    def safe_qcut(series, q, labels=False):
        try:
            return pd.qcut(series, q=q, labels=labels, duplicates='drop')
        except ValueError:
            # 当所有值都相同时，直接返回统一分值
            return pd.Series(1, index=series.index)
    
    # RFM打分
    rfm['R'] = safe_qcut(rfm['recency'], q=5) + 1
    rfm['F'] = safe_qcut(rfm['frequency'], q=5) + 1
    rfm['M'] = safe_qcut(rfm['monetary'], q=5) + 1
    
    return rfm

def build_user_profiles(df):
    """RFM模型"""
    # 首先确保将timestamp列转换为datetime类型
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    # 计算Monetary（总消费金额）
    df['monetary'] = df['avg_price'] * df['items_count']
    
    snapshot_date = df['timestamp'].max() + pd.Timedelta(days=1)
    
    rfm = df.groupby('user_name').agg({
        'timestamp': lambda x: (snapshot_date - x.max()).days,  # Recency
        'purchase_history': 'count',  # Frequency（交易次数）
        'monetary': 'sum'            # Monetary
    }).rename(columns={
        'timestamp': 'recency',
        'purchase_history': 'frequency',
        'monetary': 'monetary'
    }).reset_index()
    
    # 动态分箱函数
    def dynamic_binning(series, q=5, ascending=True):
        """动态分箱函数"""
        try:
            # 处理全零或单一值情况
            if series.nunique() <= 1:
                return pd.Series(1, index=series.index)
            
            # 确保分箱数不超过唯一值数量
            valid_q = min(q, series.nunique())
            
            # 使用百分比排名代替原始值
            ranked = series.rank(pct=True, method='first')
            
            # 生成分箱标签（确保标签数比分箱边界数少1）
            bins = pd.qcut(ranked, q=valid_q, labels=False, duplicates='drop') + 1
            
            # 处理反向分箱
            return (valid_q - bins + 1) if not ascending else bins
        
        except Exception as e:
            print(f"分箱失败: {str(e)}, 使用等宽分箱回退")
            return pd.cut(series, bins=3, labels=[1,2,3], include_lowest=True)
    
    rfm['R'] = dynamic_binning(rfm['recency'], q=5, ascending=False)
    rfm['F'] = dynamic_binning(rfm['frequency'], q=5)
    rfm['M'] = dynamic_binning(rfm['monetary'], q=5)
    
    return rfm

def identify_high_value_users_old(rfm_df, df, debug=False):
    # debug
    if debug:
        check_unhashable(df, ['user_name', 'province', 'credit_score'])
    
    """识别高价值用户"""
    # 综合评分模型
    rfm_df['hv_score'] = 0.5*rfm_df['M'] + 0.3*rfm_df['F'] + 0.2*rfm_df['R']
    
    # 合并原始数据
    high_value_users = rfm_df[rfm_df['hv_score'] >= 4].merge(
        df[['user_name', 'chinese_name', 'province', 'credit_score']].drop_duplicates(),
        on='user_name'
    )
    
    # 添加业务规则：信用分高于700
    # high_value_users = high_value_users[high_value_users['credit_score'] >= 500]
    
    return high_value_users

def identify_high_value_users(rfm_df, df, method='composite'):
    """多维度高价值用户识别"""
    # 复合评分模型
    rfm_df['score'] = (rfm_df['R']*0.2 + 
                      rfm_df['F']*0.2 + 
                      rfm_df['M']*0.6)
    
    # 业务规则过滤
    high_value = rfm_df[
        (rfm_df['score'] >= 4) &
        (rfm_df['frequency'] >= 20)
    ].merge(
        df[['user_name', 'chinese_name', "province", 'credit_score', 'income', 'is_active']].drop_duplicates(),
        on='user_name'
    )
    
    # 信用分过滤
    high_value = high_value[high_value['credit_score'] >= 650]
    
    # 收入分位数过滤
    income_threshold = high_value['income'].quantile(0.8)
    high_value = high_value[high_value['income'] >= income_threshold]
    
    return high_value.sort_values('score', ascending=False)

# 检查不可哈希类型
# 例如：列表，字典，集合等
def check_unhashable(df, columns):
    for col in columns:
        print(f"检查列: {col}")
        print(df[col])
        has_list = df[col].apply(lambda x: isinstance(x, (list, dict, set))).any()
        print(f"{col} 包含不可哈希类型: {has_list}")

def main():
    # 用户画像应用
    # rfm_scores = build_user_profiles(df)
    pass

if __name__ == "__main__":
    main()