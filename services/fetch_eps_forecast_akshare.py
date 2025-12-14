import akshare as ak
from datetime import datetime

def get_current_year_eps_forecast(stock_code):
    """获取当前年度每股收益预测均值"""
    try:
        profit_forecast = ak.stock_profit_forecast_ths(symbol=stock_code)
        
        if profit_forecast is not None and not profit_forecast.empty:
            # 获取最早年份的数据（通常是当前年度预测）
            earliest_year = profit_forecast['年度'].min()
            current_year_data = profit_forecast[profit_forecast['年度'] == earliest_year]
            
            if not current_year_data.empty:
                eps_forecast = float(current_year_data['均值'].iloc[0])
                return eps_forecast
        
        return None
        
    except Exception as e:
        print(f"获取 {stock_code} 数据失败: {e}")
        return None

def main():
    """主函数"""
    stock_code = '600900'  # 长江电力
    
    eps_forecast = get_current_year_eps_forecast(stock_code)
    
    if eps_forecast:
        print(f"{stock_code} 当前年度每股收益预测均值: {eps_forecast}元")
    else:
        print(f"{stock_code} 获取预测数据失败")

if __name__ == '__main__':
    main()