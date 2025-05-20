# visualize/visualize_time_series.py
import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
matplotlib.rcParams['font.family'] = ['Microsoft YaHei']
matplotlib.rcParams['axes.unicode_minus'] = False

def plot_monthly_category_trend(monthly_df, output_path="monthly_trend.png"):
    plt.figure(figsize=(14, 8))
    monthly_df.plot(kind='line', linewidth=2, ax=plt.gca())
    plt.title("每月各商品类别购买趋势", fontsize=16)
    plt.xlabel("月份", fontsize=12)
    plt.ylabel("购买次数", fontsize=12)
    plt.legend(title="商品类别", bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()

def plot_category_sequence_flow(seq_df, output_path="category_flow.png", top_n=15):
    top_seqs = seq_df.sort_values("count", ascending=False).head(top_n)
    plt.figure(figsize=(10, 7))
    sns.barplot(x="count", y=top_seqs.apply(lambda row: f"{row['categories_curr']} → {row['next_categories']}", axis=1),
                data=top_seqs, palette="viridis")
    plt.title("高频商品类别顺序模式", fontsize=14)
    plt.xlabel("购买顺序次数")
    plt.ylabel("类别序列")
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()
