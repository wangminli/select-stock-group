import pandas as pd
import warnings
import easyquotation

quotation = easyquotation.use('sina')  # 新浪 ['sina'] 腾讯 ['tencent', 'qq']
df = quotation.market_snapshot(prefix=True)  # prefix 参数指定返回的行情字典中的股票代码 key 是否带 sz/sh 前缀
# df = pd.DataFrame(df).T   # 加了这个速度会变慢
print(df)

# 将df转换为DataFrame并输出为CSV文件
if df:
    # 将字典转换为DataFrame，股票代码作为索引
    df_df = pd.DataFrame(df).T
    
    # 重置索引，将股票代码变成一列
    df_df = df_df.reset_index()
    df_df.rename(columns={'index': '股票代码'}, inplace=True)
    
    # 输出为CSV文件
    output_file = 'stock_market_data.csv'
    df_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"\n数据已保存到: {output_file}")
    print(f"共保存 {len(df_df)} 条股票数据")
    
    # 显示前几行数据
    print("\n前5行数据预览:")
    print(df_df.head())
else:
    print("未获取到数据")