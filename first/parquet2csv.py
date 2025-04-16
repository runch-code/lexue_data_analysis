"""
Parquet to CSV Batch Converter

Requirements:
- pandas
- pyarrow 或 fastparquet

安装依赖：pip install pandas pyarrow
"""

import pandas as pd
import os
import sys
from pathlib import Path
from typing import List
from tqdm import tqdm

def validate_parquet(file_path: Path) -> bool:
    """验证文件是否为合法Parquet文件"""
    try:
        # 快速读取元数据验证
        pd.read_parquet(file_path, engine='pyarrow', columns=[])
        return True
    except Exception as e:
        print(f"验证失败: {file_path} | 错误: {str(e)}")
        return False

def convert_parquet_to_csv(file_path: Path, output_dir: Path = None) -> None:
    """改进版转换函数（支持大文件）"""
    try:
        output_path = (output_dir/file_path.stem).with_suffix('.csv') if output_dir \
            else file_path.with_suffix('.csv')
        
        print(f"正在处理: {file_path} -> {output_path}")

        # 使用 pyarrow 直接读取
        import pyarrow.parquet as pq
        table = pq.read_table(file_path)
        
        # 分批写入（每10万行）
        batch_size = 100_000
        first_batch = True

        # 处理逻辑
        for i in tqdm(range(0, table.num_rows, batch_size)):
            batch = table.slice(i, batch_size)
            df = batch.to_pandas()
            
            df.to_csv(
                output_path,
                mode='w' if first_batch else 'a',
                header=first_batch,
                index=False,
                encoding='utf-8'
            )
            first_batch = False
            print(f"已写入: {min(i + batch_size, table.num_rows)}/{table.num_rows} 行")

    except Exception as e:
        print(f"转换失败: {file_path} | 错误: {str(e)}")
        raise


def batch_convert(file_list: List[Path], output_dir: Path = None) -> None:
    """批量转换函数"""
    # 创建输出目录
    if output_dir and not output_dir.exists():
        output_dir.mkdir(parents=True)
    
    # 进度统计
    total = len(file_list)
    success = 0
    
    for idx, file_path in enumerate(file_list, 1):
        print(f"\n[{idx}/{total}] 开始处理: {file_path}")
        
        try:
            # 文件验证
            if not validate_parquet(file_path):
                continue
                
            # 执行转换
            convert_parquet_to_csv(file_path, output_dir)
            success += 1
            
        except Exception as e:
            # 错误日志记录
            with open("conversion_errors.log", "a") as f:
                f.write(f"{file_path}\t{str(e)}\n")
    
    print(f"\n转换完成！成功: {success}/{total} 文件")

def main():
    # 命令行参数处理
    if len(sys.argv) < 2:
        print("用法: python parquet2csv.py [文件/目录]... [-o 输出目录]")
        return

    # 解析参数
    file_paths = []
    output_dir = None
    args = sys.argv[1:]
    
    i = 0
    while i < len(args):
        if args[i] == '-o':
            output_dir = Path(args[i+1])
            i += 2
        else:
            p = Path(args[i])
            if p.is_dir():
                file_paths.extend(p.glob("**/*.parquet"))
                file_paths.extend(p.glob("**/*.parq"))
            else:
                file_paths.append(p)
            i += 1
    
    # 去重处理
    file_paths = list(set(file_paths))
    
    # 过滤不存在文件
    valid_files = [p for p in file_paths if p.exists()]
    invalid_files = set(file_paths) - set(valid_files)
    
    if invalid_files:
        print(f"警告：忽略{len(invalid_files)}个无效路径")
    
    # 执行批量转换
    if valid_files:
        batch_convert(valid_files, output_dir)
    else:
        print("错误：未找到有效Parquet文件")

if __name__ == "__main__":
    main()
