# main.py
import time
import sys
from pathlib import Path
# from new import *
from load_and_preprocess import *
from visualization import *
from user_analysis import *

def main():
    # 命令行参数处理
    if len(sys.argv) < 2:
        print("用法: python main.py [文件/文件夹]... [-o 分析结果输出目录]")
        return False
    
    """命令行参数处理"""
    # 读取命令行参数
    file_paths, args, output_dir, i = [], sys.argv[1:], None, 0
    while i < len(args):
        if args[i] == '-o':
            output_dir = args[i+1]
            i += 2
        else:
            p = Path(args[i])
            if p.is_dir():
                file_paths.extend(p.glob("**/*.parquet"))
                file_paths.extend(p.glob("**/*.parq"))
                file_paths.extend(p.glob("**/*.csv"))
            else:
                file_paths.append(p)
            i += 1
    
    # 判断file_paths中文件是否为相同类型
    if len(file_paths) > 1:
        file_types = set(p.suffix for p in file_paths)
        if len(file_types) > 1:
            print("警告：混合文件类型，可能导致读取错误")
            return False
    
    # 输出目录处理
    if output_dir:
        output_dir = Path(output_dir)
        if not output_dir.exists():
            output_dir.mkdir(parents=True, exist_ok=True) # 创建输出目录
    else:
        print("未指定输出目录，将使用当前工作目录")
    
    # 去重处理
    file_paths = list(set(file_paths))
    # 过滤不存在文件
    valid_files = [p for p in file_paths if p.exists()] # 有效文件列表
    invalid_files = set(file_paths) - set(valid_files)
    if invalid_files:
        print(f"警告：忽略{len(invalid_files)}个无效路径")
    
    """数据加载"""
    start_time = time.time()
    # 判断文件类型
    if len(valid_files) == 1:
        file_path = valid_files[0]
        if file_path.suffix in ['.parquet', '.parq']:
            print("正在读取单个parquet文件...")
            # 读取单个文件
            df = load_parquet_data([file_path], if_file_pattern=False) # 读取数据
        elif file_path.suffix == '.csv':
            print("正在读取单个csv文件...")
            # 读取单个文件
            df = load_csv_data([file_path], if_file_pattern=False) # 读取数据
        else:
            print(f"警告：不支持的文件类型 {file_path.suffix}")
            return False
    else:
        # 读取多个文件
        if valid_files[0].suffix in ['.parquet', '.parq']:
            print("正在读取多个parquet文件...")
            # 读取多个文件
            df = load_parquet_data(valid_files, if_file_pattern=False) # 读取数据
        elif valid_files[0].suffix == '.csv':
            print("正在读取多个csv文件...")
            # 读取多个文件
            df = load_csv_data(valid_files, if_file_pattern=False) # 读取数据
        else:
            print(f"警告：不支持的文件类型 {valid_files[0].suffix}")
            return False
    load_time = time.time() - start_time
    
    """正式分析流程"""
    # 执行分析流程
    plot_province_distribution(df, base_dir=output_dir) # 地域分布热力图
    plot_consumption_analysis(df, base_dir=output_dir) # 消费分析三联图
    
    rfm = build_user_profiles(df) # 用户画像构建
    hv_users = identify_high_value_users(rfm, df) # 高价值用户识别
    
    # 保存结果
    if output_dir:
        hv_users.to_csv(output_dir / 'hv_users.csv', index=False)
        print(f"RFM分析结果已保存到 {output_dir / 'hv_users.csv'}")
    else:
        hv_users.to_csv("./high_value_users.csv", index=False)
        print("高价值用户数据已保存到 high_value_users.csv")

    # 显示运行时间
    end_time = time.time() - start_time
    print(f"数据加载时间: {load_time:.2f}秒")
    print(f"总运行时间: {end_time:.2f}秒")
    print(f"数据分析用时: {end_time - load_time:.2f}秒")
    
    return True

if __name__ == "__main__":
    main()