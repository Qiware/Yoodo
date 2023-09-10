import akshare
import talib

df = akshare.stock_zh_a_hist(symbol="000001", period="daily", start_date="20230601", end_date="20230907", adjust="")


print(df.head())

print(type(df["收盘"]))

ma = talib.MA(df["收盘"], timeperiod=5)

print(ma)
