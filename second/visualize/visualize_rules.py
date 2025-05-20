# visualize/visualize_rules.py
import matplotlib.pyplot as plt
import networkx as nx
import pandas as pd
import matplotlib

# 设置中文字体支持
matplotlib.rcParams['font.family'] = ['Microsoft YaHei']
matplotlib.rcParams['axes.unicode_minus'] = False

def visualize_association_rules(rules, top_k=20, output_path="rules_network.png"):
    """将关联规则绘制为网络图"""
    if rules.empty:
        print(f"[可视化跳过] 无可用规则，未生成图表: {output_path}")
        return

    G = nx.DiGraph()
    rules = rules.sort_values('lift', ascending=False).head(top_k)

    for _, row in rules.iterrows():
        lhs = ', '.join(row['antecedents']) if isinstance(row['antecedents'], set) else str(row['antecedents'])
        rhs = ', '.join(row['consequents']) if isinstance(row['consequents'], set) else str(row['consequents'])
        G.add_edge(lhs, rhs, weight=row['confidence'], lift=row['lift'])

    edge_weights = [G[u][v]['weight'] for u, v in G.edges()]
    if not edge_weights:
        print(f"[可视化跳过] 无有效边生成网络图: {output_path}")
        return

    fig, ax = plt.subplots(figsize=(14, 10))
    pos = nx.spring_layout(G, k=0.8, seed=42)

    # 使用更明显的边颜色和宽度映射
    edges = nx.draw_networkx_edges(
        G, pos, ax=ax,
        edge_color=edge_weights,
        edge_cmap=plt.cm.Oranges,
        edge_vmin=min(edge_weights),
        edge_vmax=max(edge_weights),
        width=[2 + 4 * w for w in edge_weights],  # 加粗边
        arrows=True,
        arrowstyle='-|>',
        arrowsize=20
    )

    # 使用支持中文的标签
    nx.draw_networkx_labels(G, pos, ax=ax, font_size=12, font_family='Microsoft YaHei')
    nx.draw_networkx_nodes(G, pos, node_color='lightblue', node_size=2500, ax=ax)

    sm = plt.cm.ScalarMappable(cmap=plt.cm.Oranges,
                               norm=plt.Normalize(vmin=min(edge_weights), vmax=max(edge_weights)))
    sm._A = []
    fig.colorbar(sm, ax=ax, label='Confidence')
    ax.set_title("高置信度关联规则网络图", fontsize=16)
    ax.axis('off')
    plt.tight_layout()
    plt.savefig(output_path, dpi=300)
    plt.close()