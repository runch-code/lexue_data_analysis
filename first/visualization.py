from pyecharts.charts import Map, Bar, Line
from pyecharts import options as opts
import matplotlib.pyplot as plt
import matplotlib.patheffects as pe
import pandas as pd
import numpy as np
from scipy.stats import gaussian_kde
import warnings
import seaborn as sns
from scipy.ndimage import gaussian_filter1d  # 添加高斯滤波依赖

def plot_province_distribution(df, base_dir=None):
    """地域分布热力图"""
    province_count = df['province'].value_counts()
    # 将省份名称和数量转换为字典
    province_count = list(zip(province_count.index, province_count.values.tolist()))
    province_count = [(province, count) for province, count in province_count if count > 0]
    # print(province_count)
    # 提取count中的最大值
    count = [count for _, count in province_count]
    max_count, min_count = max(count), min(count)
    # max_count = max([count for _, count in province_count])
    # print(province_count)
    
    m = Map()
    m.add("用户分布", 
          province_count,
          "china")
    # m.set_global_opts(title_opts=opts.TitleOpts(title="用户地域分布"))
     # 设置全局配置
    m.set_global_opts(
        title_opts=opts.TitleOpts(
            title="用户地域分布",
            subtitle="数据来源：乐学数据分析课程",
        ),
        visualmap_opts=opts.VisualMapOpts(
            min_=min_count,  # 自动获取最小值
            max_=max_count,  # 自动获取最大值
            is_piecewise=False,  # 连续型视觉映射
            range_color=["#FFE4E1", "#FF6347"],  # 颜色从浅粉到深红
            pos_left="10%",  # 控制组件位置
            pos_bottom="20%"
        ),
        tooltip_opts=opts.TooltipOpts(
            trigger="item",
            formatter="{b}<br/>用户数量：{c}"
        )
    )
    
    # 设置系列配置
    m.set_series_opts(
        itemstyle_opts={
            "borderColor": "#fff",  # 区域边界颜色
            "borderWidth": 0.5      # 边界宽度
        }
    )
    if base_dir:
        m.render(path=f"{base_dir}/province_distribution.html")
    else:
        m.render(path="province_distribution.html")

def plot_price_distribution(df, base_dir=None):
# 配色方案设置
    COLORS = {
        'hist': "#000066",    # 深蓝色柱形
        'kde': '#cc0000',     # 红色曲线
        'median': '#e67e22',  # 橙色中位线
        'text': '#2c3e50',    # 深灰文字
        'bg': '#ecf0f1'       # 浅灰背景
    }
    
    # 创建画布
    plt.figure(figsize=(12, 7), facecolor=COLORS['bg'])
    ax = plt.gca()
    
    # 分箱与绘图
    prices = df['avg_price'].dropna()
    bins = np.linspace(prices.min(), prices.quantile(0.95), 15)
    
    # 使用对比色方案
    plt.hist(prices, bins=bins, 
            density=True,    # 启用密度模式
            edgecolor='black',
            color=COLORS['hist'], 
            alpha=0.7)
    # ===== KDE曲线独立绘制 =====
    sns.kdeplot(prices, color=COLORS['kde'], 
               linewidth=3, linestyle='-',
               bw_method='scott',   # 自动带宽计算
               gridsize=200)        # 提高曲线平滑度
    
    # 标注设置
    median_price = prices.median()
    ax.axvline(median_price, color=COLORS['median'], 
              linestyle='--', linewidth=2.5, alpha=0.9)
    ax.text(median_price*1.05, ax.get_ylim()[1]*0.8, 
           f'中位数 ¥{median_price:.0f}', 
           color=COLORS['text'], fontsize=12,
           bbox=dict(facecolor='white', alpha=0.8))
    
    # 自动标注密集区间
    mode_price = prices.mode()[0]
    ax.annotate(f'最密集区间\n¥{mode_price:.0f}±{bins[1]-bins[0]:.0f}',
                xy=(mode_price, ax.get_ylim()[1]*0.6),
                xytext=(mode_price*1.2, ax.get_ylim()[1]*0.5),
                arrowprops=dict(arrowstyle='->', color='#34495e'),
                bbox=dict(boxstyle='round', alpha=0.9, facecolor='white'))
    
    # 图表美化
    plt.title('客单价分布核心趋势', color=COLORS['text'], pad=20, fontsize=16)
    plt.xlabel('价格区间（元）', color=COLORS['text'], fontsize=12)
    plt.ylabel('概率密度', color=COLORS['text'], fontsize=12)
    
    # 坐标轴颜色统一
    ax.tick_params(colors=COLORS['text'], which='both')
    for spine in ax.spines.values():
        spine.set_color(COLORS['text'])
        
    plt.grid(axis='y', alpha=0.3, color=COLORS['text'])
    
    # 坐标轴优化
    ax.set_xlim(0, 1000)
    ax.xaxis.set_major_formatter('¥{x:,.0f}')
    plt.xticks(np.linspace(0, 1000, 6))
    
    plt.tight_layout()
    if base_dir:
        plt.savefig(f"{base_dir}/price_distribution.png", dpi=300, bbox_inches='tight')
    else:
        plt.savefig('price_distribution.png', dpi=300, bbox_inches='tight')
    plt.close()

# ===== 用户活跃时段分析（增强对比度版） =====
def plot_activity_timeline(df, base_dir=None):
    # 专业配色方案
    COLORS = {
        'fill': '#2ecc71',      # 填充主色
        'line': '#27ae60',      # 曲线颜色
        'peak': '#e74c3c',      # 峰值强调色
        'peak_fill': '#f39c12', # 高峰时段色
        'night_fill': '#3498db',# 晚间时段色
        'text': '#2c3e50',      # 文字颜色
        'bg': '#f8f9fa'         # 背景颜色
    }

    plt.figure(figsize=(14, 7), facecolor=COLORS['bg'])
    ax = plt.gca()

    # 数据处理
    time_data = pd.to_datetime(df['timestamp']).dt.floor('H').dt.hour
    hourly_count = time_data.value_counts().sort_index()

    # 动态Y轴范围调整（保留10%头部空间）
    y_min, y_max = hourly_count.min(), hourly_count.max()
    y_padding = (y_max - y_min) * 0.1
    ax.set_ylim(y_min - y_padding, y_max + y_padding)

    # 高级平滑处理
    from scipy.signal import savgol_filter
    smoothed = savgol_filter(hourly_count.values, 
                            window_length=5, 
                            polyorder=3)

    # 增强对比度可视化组件
    # 1. 渐变填充增强深度感知
    gradient = np.linspace(0, 1, 256).reshape(1, -1)
    gradient = np.vstack((gradient, gradient))
    ax.imshow(gradient, aspect='auto', cmap=plt.cm.Greens, 
             extent=[-0.5, 23.5, y_min, y_max], 
             alpha=0.15, zorder=0)

    # 2. 主曲线增强
    ax.plot(hourly_count.index, smoothed, 
           color=COLORS['line'], lw=4, 
           marker='o', markersize=10, markerfacecolor='white',
           zorder=3, path_effects=[pe.Stroke(linewidth=6, foreground='#ffffff'), pe.Normal()])

    # 3. 对比度刻度系统
    ax.yaxis.set_major_locator(plt.MaxNLocator(10))  # 增加主刻度密度
    ax.yaxis.set_minor_locator(plt.AutoMinorLocator(5))  # 添加次要刻度
    ax.tick_params(axis='y', which='both', labelsize=10, colors=COLORS['text'])
    ax.spines['left'].set_linewidth(1.5)

    # 动态峰值标注（智能避让）
    peak_hour = hourly_count.idxmax()
    ax.plot(peak_hour, hourly_count.max(), 'o', 
           ms=14, mec=COLORS['peak'], mfc='white', mew=2, zorder=4)
    ax.annotate(f'峰值时段: {peak_hour:02d}:00\n活跃用户: {hourly_count.max():,}',
               xy=(peak_hour, hourly_count.max()),
               xytext=(10, ax.get_ylim()[1]*0.8) if peak_hour < 12 else (14, ax.get_ylim()[1]*0.8),
               arrowprops=dict(arrowstyle='->', color=COLORS['peak'], lw=2,
                               connectionstyle="arc3,rad=0.2"),
               bbox=dict(boxstyle='round', facecolor='white', alpha=0.95),
               fontsize=11, color=COLORS['text'], zorder=5)

    # 时段区间着色增强
    ax.axvspan(11, 14, color=COLORS['peak_fill'], alpha=0.15, label='午间高峰')
    ax.axvspan(18, 21, color=COLORS['night_fill'], alpha=0.15, label='晚间高峰')
    
    # 专业级标签系统
    ax.set_title('用户活跃时段分布热力分析', fontsize=16, pad=20, color=COLORS['text'])
    ax.set_xlabel('时间（小时）', fontsize=12, color=COLORS['text'], labelpad=15)
    ax.set_ylabel('活跃用户数', fontsize=12, color=COLORS['text'], labelpad=15)
    
    # 高级坐标轴配置
    ax.set_xticks(np.arange(0, 24, 2))
    ax.set_xticks(np.arange(0, 24, 1), minor=True)
    ax.xaxis.set_tick_params(which='major', length=6, width=1.2, colors=COLORS['text'])
    ax.set_xlim(-0.5, 23.5)
    
    # 刻度值格式化
    def hour_formatter(x, pos):
        return f"{int(x):02d}:00"
    ax.xaxis.set_major_formatter(plt.FuncFormatter(hour_formatter))
    
    # 图例增强
    ax.legend(loc='upper right', frameon=True, 
             facecolor='white', edgecolor=COLORS['text'], 
             title='高峰时段', title_fontsize=10)

    plt.tight_layout()
    save_path = f"{base_dir}/activity_timeline_enhanced.png" if base_dir else 'activity_timeline_enhanced.png'
    plt.savefig(save_path, dpi=300, bbox_inches='tight', facecolor=COLORS['bg'])
    plt.close()

def plot_consumption_analysis(df, base_dir=None):
    # 样式初始化
    plt.style.use('seaborn-v0_8-darkgrid')
    plt.rcParams.update({
        'font.sans-serif': ['Microsoft YaHei', 'SimHei'],
        'axes.unicode_minus': False,
        'figure.dpi': 150,
        'axes.titlesize': 14,
        'axes.titleweight': 'bold'
    })

    """客单价分布可视化"""
    '''plt.figure(figsize=(12, 7), facecolor='#f5f5f5')
    ax = plt.gca()
    
    # 优化分箱策略
    prices = df['avg_price'].dropna()
    bins = np.linspace(prices.min(), prices.quantile(0.95), 15)  # 聚焦95%的数据
    
    # 直方图 + KDE曲线
    sns.histplot(prices, bins=bins, kde=True, 
                color='#2ecc71', edgecolor='white',
                alpha=0.8)
    
    # 标注核心区间
    median_price = prices.median()
    ax.axvline(median_price, color='#e74c3c', linestyle='--', linewidth=2)
    ax.text(median_price*1.05, ax.get_ylim()[1]*0.8, 
           f'中位数 ¥{median_price:.0f}', 
           color='#e74c3c', fontsize=12)
    
    # 自动标注密集区间
    mode_price = prices.mode()[0]
    ax.annotate(f'最密集区间\n¥{mode_price:.0f}±{bins[1]-bins[0]:.0f}',
                xy=(mode_price, ax.get_ylim()[1]*0.6),
                xytext=(mode_price*1.2, ax.get_ylim()[1]*0.5),
                arrowprops=dict(arrowstyle='->', color='#34495e'),
                bbox=dict(boxstyle='round', alpha=0.9, facecolor='white'))
    
    # 图表美化
    plt.title('客单价核心分布分析', pad=20)
    plt.xlabel('价格区间（元）', fontsize=12)
    plt.ylabel('订单数量', fontsize=12)
    plt.grid(axis='y', alpha=0.4)
    
    plt.tight_layout()
    plt.savefig('price_simple.png', dpi=150, bbox_inches='tight')
    plt.close()'''

    # ===== 品类销售分析 =====
    plt.figure(figsize=(12, 7), facecolor='#f8f9fa')
    ax = plt.gca()
    flag = False
    
    category_data = df.groupby('category')['avg_price'].sum().nlargest(10).sort_values()
    
    # 当数值过大时，降低category_data的数量级（使用亿元为单位）
    if category_data.max() > 100000000:
        flag = True
        category_data = category_data / 100000000
    # category_data = category_data / 100000000
    
    # 使用渐变颜色条
    cmap = plt.cm.get_cmap('Blues_r', len(category_data))
    colors = [cmap(i) for i in range(len(category_data))]
    colors = colors[::-1]  # 反转颜色条
    
    bars = plt.barh(category_data.index.astype(str), category_data.values,
                   color=colors, edgecolor='grey')
    
    # 自动调整标签
    ax.set_yticklabels([label.get_text()[:18]+'...' 
                      if len(label.get_text())>20 else label.get_text() 
                      for label in ax.get_yticklabels()])
    
    # 动态数据标签
    max_val = category_data.max()
    for bar in bars:
        width = bar.get_width()
        label_x = width + max_val*0.02
        label_text = f'¥{width:,.0f}'
        color = '#2c3e50' if width > max_val*0.3 else '#7f8c8d'
        plt.text(label_x, bar.get_y()+bar.get_height()/2, 
                label_text, va='center', color=color, fontsize=10)

    plt.title('品类销售额分析\n(Category sales analysis)', pad=20)
    if flag:
        plt.xlabel('销售额（亿元）', labelpad=12)
    else:
        plt.xlabel('销售额（元）', labelpad=12)
    plt.grid(axis='x', alpha=0.4)
    
    plt.tight_layout()
    if base_dir:
        plt.savefig(f"{base_dir}/category_sales.png", bbox_inches='tight')
    else:
        plt.savefig('category_sales.png', bbox_inches='tight')
    plt.close()

    # ===== 用户活跃时段分析 =====
    '''plt.figure(figsize=(12, 6), facecolor='#f8f9fa')
    ax = plt.gca()
    
    time_data = pd.to_datetime(df['timestamp']).dt.floor('h').dt.hour
    hourly_count = time_data.value_counts().sort_index()
    
    # 平滑曲线处理
    from scipy.ndimage import gaussian_filter1d
    smoothed = gaussian_filter1d(hourly_count.values, sigma=1.2)
    
    # 面积图+折线图组合
    plt.fill_between(hourly_count.index, smoothed, 
                    color='#2ecc71', alpha=0.2)
    plt.plot(hourly_count.index, smoothed, 
            color='#27ae60', lw=3, marker='o', markersize=8)
    
    # 峰值标注增强
    peak_hour = hourly_count.idxmax()
    plt.annotate(f'活跃峰值：{peak_hour}:00\n用户数：{hourly_count.max()}',
                xy=(peak_hour, hourly_count.max()),
                xytext=(peak_hour%12+3, hourly_count.max()*0.7),
                arrowprops=dict(arrowstyle='->', color='#e74c3c', lw=1.5),
                bbox=dict(boxstyle='round', facecolor='white', alpha=0.9))

    # 时段区间着色
    plt.axvspan(11, 14, color='#f39c12', alpha=0.1, label='午间高峰')
    plt.axvspan(18, 21, color='#9b59b6', alpha=0.1, label='晚间高峰')
    
    plt.title('用户活跃时段分布\n(User Activity Timeline)', pad=20)
    plt.xlabel('时间（小时）', labelpad=12)
    plt.ylabel('活跃用户数', labelpad=12)
    plt.xticks(range(0, 24, 2))
    plt.xlim(-0.5, 23.5)
    plt.legend()
    
    plt.tight_layout()
    if base_dir:
        plt.savefig(f"{base_dir}/activity_timeline.png", bbox_inches='tight')
    else:
        plt.savefig('activity_timeline.png', bbox_inches='tight')
    plt.close()'''

    print("图表已生成：price_distribution.png, category_sales.png, activity_timeline.png")

def main():
    # 执行可视化
    # plot_province_distribution(df)
    # plot_consumption_analysis(df)
    pass
    
if __name__ == "__main__":
    main()