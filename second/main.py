# main.py
import time
import sys
from pathlib import Path
from preprocess.load_and_preprocess import load_csv_data, load_parquet_data
from preprocess.parse_catalog import load_product_catalog, extract_categories, extract_prices
from analysis.association_analysis import *
from analysis.time_series_analysis import *
from analysis.refund_analysis import *
from visualize.visualize_rules import visualize_association_rules
from visualize.visualize_time_series import plot_monthly_category_trend, plot_category_sequence_flow

def main():
    if len(sys.argv) < 3:
        print("用法: python main.py [数据文件/目录] [商品目录JSON路径] [-o 输出目录]")
        return

    # 参数解析
    file_paths, args, output_dir, catalog_path, i = [], sys.argv[1:], None, None, 0
    catalog_path = args[1]  # 第二个参数必须为商品目录路径

    while i < len(args):
        if args[i] == '-o':
            output_dir = Path(args[i+1])
            output_dir.mkdir(parents=True, exist_ok=True)
            i += 2
        else:
            p = Path(args[i])
            if p.is_dir():
                file_paths.extend(p.glob("**/*.parquet"))
                file_paths.extend(p.glob("**/*.csv"))
            elif p.suffix in ['.csv', '.parquet', '.parq']:
                file_paths.append(p)
            i += 1

    valid_files = [f for f in file_paths if f.exists()]
    if not valid_files:
        print("未找到有效数据文件")
        return

    start_time = time.time()

    # 读取数据
    if valid_files[0].suffix in ['.parquet', '.parq']:
        df = load_parquet_data(valid_files)
    elif valid_files[0].suffix == '.csv':
        df = load_csv_data(valid_files)
    else:
        print("不支持的文件格式")
        return

    # 输出读取数据时间
    print(f"数据读取完成，耗时: {time.time() - start_time:.2f} 秒")
    # print(df)
    
    id2cat, id2price = load_product_catalog(catalog_path)
    df['categories'] = df['purchase_history'].apply(lambda x: extract_categories(x, id2cat))
    df['prices'] = df['purchase_history'].apply(extract_prices)

    # === 分析任务 1：商品类别关联规则挖掘 ===
    print("正在执行任务1：商品类别关联规则挖掘...")
    transactions = prepare_category_transactions(df)
    rules = run_apriori(transactions)
    electronics_rules = filter_rules_by_category(rules, "车载电子")
    print(f"共生成规则数：{len(rules)}，电子产品相关规则数：{len(electronics_rules)}")
    visualize_association_rules(rules, output_path=str(output_dir / "electronics_rules.png"))

    # === 任务2：支付方式与商品类别关联 ===
    print("正在执行任务2：支付方式与高价分析...")
    payment_rules = analyze_payment_category_rules(df)
    visualize_association_rules(payment_rules, output_path=str(output_dir / "payment_rules.png"))
    hv_payment = analyze_high_value_payment(df)
    hv_payment.to_csv(output_dir / "high_value_payment.csv", index=False)

    # === 任务3：时间序列模式 ===
    print("正在执行任务3：时间序列趋势分析...")
    df = add_time_features(df)
    monthly_trend = monthly_category_trend(df)
    plot_monthly_category_trend(monthly_trend, output_path=output_dir / "monthly_trend.png")
    # category_seq = sequential_category_patterns(df)
    # plot_category_sequence_flow(category_seq, output_path=output_dir / "category_flow.png")

    # === 任务4：退款组合模式 ===
    print("正在执行任务4：退款组合分析...")
    refund_rules = analyze_refund_combinations(df)
    visualize_association_rules(refund_rules, output_path=output_dir / "refund_rules.png")

    print("所有分析任务完成！")
    print(f"总运行时间: {time.time() - start_time:.2f} 秒")

if __name__ == "__main__":
    main()
