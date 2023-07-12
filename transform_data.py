import pandas as pd

def read_data(path):
    months = ['April', 'August', 'December', 'February', 'January', 'July', 'June', 'March', 'May', 'November', 'October', 'September']
    data_frames = []
    for i in months:
        df = pd.read_csv(f'{path}/Sales_{i}_2019.csv')
        data_frames.append(df)
    
    merged_df = pd.concat(data_frames)
    merged_df.dropna(inplace=True)
    # print(merged_df.shape)
    merged_df.reset_index(drop=True, inplace=True)
    merged_df = merged_df.drop(merged_df[merged_df['OrderID'] == 'Order ID'].index)
    # print(merged_df.shape)

    return merged_df

def transform_data(df):
    df['Datekey'] = df['OrderDate'].apply(lambda x: x.split(' ')[0]).astype('str')
    df['Revenue'] = df['PriceEach'].astype('float') * df['QuantityOrdered'].astype('int')
    df['QuantityOrdered'] = df['QuantityOrdered'].astype('int')
    grouped_quantity = df.groupby(['Datekey', 'Product'])['QuantityOrdered'].sum()
    grouped_revenue = df.groupby(['Datekey', 'Product'])['Revenue'].sum()
    grouped_data = pd.merge(grouped_quantity, grouped_revenue, on=['Datekey', 'Product']).reset_index()

    return grouped_data

if __name__ == '__main__':
    
    path = './data'
    df = read_data(path)
    print(df.head())
    df = transform_data(df)
    print(df)
    with open(f'{path}/transformed_data.json', 'w') as f:
        df.to_json(f, orient='records')
        print(df)
        f.close()
        
