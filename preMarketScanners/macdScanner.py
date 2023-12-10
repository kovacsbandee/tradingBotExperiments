import os
import pandas as pd


DB_PATH = 'F:/tradingActionExperiments_database'

files = os.listdir(f'{DB_PATH}/stockwise_database')

f = 'TSLA.csv'
price_col = 'open'
resampled_price_col = f'{price_col}_resampled'

sticker_df = pd.read_csv(f'{DB_PATH}/stockwise_database/{f}', index_col='Datetime')
sticker_df.index.name = 'Datetime'
sticker_df.index = pd.to_datetime(sticker_df.index, utc=True)
temp = pd.DataFrame(pd.Series(sticker_df.index.astype('str')).str.split(' ', expand=True))
temp.columns = ['date', 'time']
temp.index = sticker_df.index
sticker_df = pd.merge(sticker_df,
                      temp,
                      left_index=True,
                      right_index=True,
                      how='left')


resampled_price = sticker_df[price_col].resample('1D').mean()
resampled_price.name = resampled_price_col
resampled_price.index = resampled_price.index.astype('str')
resampled_price = pd.DataFrame(resampled_price)
resampled_price = resampled_price[~resampled_price[resampled_price_col].isna()]
resampled_price.reset_index(inplace=True)
resampled_price[['date', 'time']] = resampled_price.Datetime.str.split(' ', expand=True)

resampled_price['MACD_ema'] = \
    resampled_price[resampled_price_col].ewm(span=12, adjust=False, min_periods=12).mean() - \
    resampled_price[resampled_price_col].ewm(span=26, adjust=False, min_periods=26).mean()

resampled_price['MACD_s'] = \
    resampled_price['MACD_ema'].ewm(span=9, adjust=False, min_periods=9).mean()

resampled_price['MACD'] = resampled_price['MACD_ema'] - resampled_price['MACD_s']
resampled_price['macd_selector'] = resampled_price['MACD'] - resampled_price['MACD_s']


sticker_df = \
    pd.merge(sticker_df,
             resampled_price[[resampled_price_col, 'date']],
             how='left',
             left_on='date',
             right_on='date')
sticker_df.drop(['date', 'time'], axis=1, inplace=True)



sticker_df['MACD_ema'] = \
    sticker_df[price_col].ewm(span=12, adjust=False, min_periods=12).mean() - \
    sticker_df[price_col].ewm(span=26, adjust=False, min_periods=26).mean()

sticker_df['MACD_ema'] = sticker_df['MACD_ema'].rolling(window=3 * 60, center=False).mean()
sticker_df['MACD_s'] = \
    sticker_df['MACD_ema'].ewm(span=9, adjust=False, min_periods=9).mean()

sticker_df['MACD'] = sticker_df['MACD_ema'] - sticker_df['MACD_s']
