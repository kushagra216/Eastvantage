import sqlite3
import pandas as pd
import os

DATABASE_NAME = 'assessment_data.db'
OUTPUT_FILE = 'test_output.csv'

def create_dummy_database():
    # if os.path.exists(DATABASE_NAME):
    #     os.remove(DATABASE_NAME)
    
    conn = sqlite3.connect(DATABASE_NAME)
    cn = conn.cursor()

    # Creating all 4 tables schema 
    cn.execute('CREATE TABLE Customer (customer_id INTEGER PRIMARY KEY, age INTEGER)')
    cn.execute('CREATE TABLE Sales (sales_id INTEGER PRIMARY KEY, customer_id INTEGER)')
    cn.execute('CREATE TABLE Items (item_id INTEGER PRIMARY KEY, item_name TEXT)')
    cn.execute('''
        CREATE TABLE Orders (
            order_id INTEGER PRIMARY KEY, 
            sales_id INTEGER, 
            item_id INTEGER, 
            quantity INTEGER
        )
    ''')

    # Items test cases x=1, y=2, z=3
    cn.executemany('INSERT INTO Items VALUES (?,?)', [(1, 'x'), (2, 'y'), (3, 'z')])
    
    # Creating test case for 21 year customer with 10 as total qunatity.
    cn.execute('INSERT INTO Customer VALUES (1, 21)')
    cn.execute('INSERT INTO Sales VALUES (101, 1)')
    cn.execute('INSERT INTO Orders VALUES (1001, 101, 1, 10)') 

    # Creating test case for 23 year customer with one purchase of each item.
    cn.execute('INSERT INTO Customer VALUES (2, 23)')
    cn.execute('INSERT INTO Sales VALUES (102, 2)')
    cn.executemany('INSERT INTO Orders VALUES (?,?,?,?)', [
        (1002, 102, 1, 1), 
        (1003, 102, 2, 1), 
        (1004, 102, 3, 1)  
    ])

    # Creating test case for 3rd customer
    cn.execute('INSERT INTO Customer VALUES (3, 35)')
    cn.execute('INSERT INTO Sales VALUES (103, 3)')
    cn.execute('INSERT INTO Orders VALUES (1005, 103, 3, 2)') 

    # Creating test case for over aged customer to test filters
    cn.execute('INSERT INTO Customer VALUES (4, 40)')
    cn.execute('INSERT INTO Sales VALUES (104, 4)')
    cn.execute('INSERT INTO Orders VALUES (1006, 104, 1, 5)')

    conn.commit()
    conn.close()
    print(f"Mock database '{DATABASE_NAME}' created with test data.")

# Creating a function for each solution to later compare the results
def solution_pure_sql(db_path):
    """
    SQL IMPLEMENTATIOON
    """
    conn = sqlite3.connect(db_path)
    
    query = """
    SELECT 
        c.customer_id as Customer,c.age as Age,i.item_name as Item,
        CAST(SUM(IFNULL(o.quantity, 0)) AS INTEGER) as Quantity
    FROM Customer c
    JOIN Sales s ON c.customer_id = s.customer_id
    JOIN Orders o ON s.sales_id = o.sales_id
    JOIN Items i ON o.item_id = i.item_id
    WHERE c.age BETWEEN 18 AND 35
    GROUP BY c.customer_id, i.item_name
    HAVING Quantity > 0
    ORDER BY c.customer_id, i.item_name;
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def solution_pandas(db_path):
    conn = sqlite3.connect(db_path)
    
    # Extract raw tables
    cust_df = pd.read_sql("SELECT * FROM Customer", conn)
    sales_df = pd.read_sql("SELECT * FROM Sales", conn)
    orders_df = pd.read_sql("SELECT * FROM Orders", conn)
    items_df = pd.read_sql("SELECT * FROM Items", conn)
    conn.close()

    # Merge DataFrames (Equivalent to SQL JOINS)
    merged = (
        cust_df.merge(sales_df, on='customer_id')
               .merge(orders_df, on='sales_id')
               .merge(items_df, on='item_id')
    )

    # Filter Age 18-35 
    filtered = merged[(merged['age'] >= 18) & (merged['age'] <= 35)]

    # Handle Nulls: Fill NaN quantity with 0 
    filtered['quantity'] = filtered['quantity'].fillna(0)

    # Group by Customer, Age, and Item, then Sum Quantity 
    result = filtered.groupby(['customer_id', 'age', 'item_name'], as_index=False)['quantity'].sum()

    # Filter out 0 quantities 
    result = result[result['quantity'] > 0]

    # Convert to Integer (No decimals) 
    result['quantity'] = result['quantity'].astype(int)

    # Rename columns to match output requirements 
    result = result.rename(columns={
        'customer_id': 'Customer',
        'age': 'Age',
        'item_name': 'Item',
        'quantity': 'Quantity'
    })

    return result

def main():

    create_dummy_database()

    # Execute SQL Solution
    print("Running SQL Solution...")
    df_sql = solution_pure_sql(DATABASE_NAME)
    print(df_sql)

    # Execute Pandas Solution
    print("\nRunning Pandas Solution...")
    df_pandas = solution_pandas(DATABASE_NAME)
    print(df_pandas)

    # Sorting before comparing
    df_sql_sorted = df_sql.sort_values(by=['Customer', 'Item']).reset_index(drop=True)
    df_pandas_sorted = df_pandas.sort_values(by=['Customer', 'Item']).reset_index(drop=True)

    if df_sql_sorted.equals(df_pandas_sorted):
        print("\n Pass BOth solution have same results.")
        
        # Store to CSV with semicolon delimiter.
        df_sql.to_csv(OUTPUT_FILE, sep=';', index=False)
        print(f"Output saved in {OUTPUT_FILE}")
    else:
        print("\n Fail Different results")

if __name__ == "__main__":
    main()
