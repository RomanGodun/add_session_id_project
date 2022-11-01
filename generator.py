import pandas as pd
import numpy as np

import sys

n_customers = 1_000
n_products = 1_000_000
n_rows = 100_000_000

start = "2022-01-01 00:00:00"
end = "2023-01-01 00:00:00"

file_path = "your/path/to/file/data.csv"

def generate_csv(n_customers ,n_products, n_rows, start, end, file_path):
    
    random_data = {
        "customer_id":np.random.randint(1,n_customers,size=n_rows, dtype = 'uint32'),
        "product_id":np.random.randint(1,n_products,size=n_rows, dtype = 'uint64')
    }
    df = pd.DataFrame(random_data)
    
    start_u = pd.to_datetime(start).value // 10**9
    end_u = pd.to_datetime(end).value // 10**9

    df["timestamp"] = pd.to_datetime(np.random.randint(start_u, end_u, n_rows), unit="s")
    
    df.to_csv(file_path, index=False)  

if __name__ == "__main__":
    generate_csv(n_customers ,n_products, n_rows, start, end, file_path)

