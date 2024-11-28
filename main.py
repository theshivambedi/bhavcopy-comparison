import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime

# Function to initialize the database
def init_db():
    conn = sqlite3.connect('bhavcopy.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS bhavcopy
                 (date TEXT, contract_d TEXT, previous_s REAL, open_price REAL, high_price REAL, 
                 low_price REAL, close_price REAL, settlement REAL, net_change REAL, 
                 oi_no_con REAL, traded_qua REAL, trd_no_con REAL, traded_val REAL,
                 PRIMARY KEY (date, contract_d))''')
    conn.commit()
    conn.close()

# Function to insert data into the database
def insert_data(df):
    conn = sqlite3.connect('bhavcopy.db')
    try:
        df.to_sql('bhavcopy', conn, if_exists='append', index=False)
    except sqlite3.IntegrityError:
        # Handle duplicate entries
        cursor = conn.cursor()
        for _, row in df.iterrows():
            try:
                cursor.execute('''INSERT OR REPLACE INTO bhavcopy 
                                  (date, contract_d, previous_s, open_price, high_price, 
                                   low_price, close_price, settlement, net_change, 
                                   oi_no_con, traded_qua, trd_no_con, traded_val) 
                                  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', 
                                tuple(row))
            except Exception as e:
                st.error(f"Error inserting row: {e}")
        conn.commit()
    conn.close()

# Function to process the uploaded CSV file
def process_csv(file):
    df = pd.read_csv(file)
    
    # Extract date from filename
    filename = file.name
    date_str = filename[2:8]  # Extract DDMMYY from the filename
    date = datetime.strptime(date_str, '%d%m%y').strftime('%Y-%m-%d')
    df['date'] = date
    
    # Remove first 6 characters from CONTRACT_D
    df['CONTRACT_D'] = df['CONTRACT_D'].str[6:]
    
    # Ensure numeric conversion
    numeric_columns = ['NET_CHANGE', 'OI_NO_CON', 'OPEN_PRICE', 'HIGH_PRICE', 
                       'LOW_PRICE', 'CLOSE_PRIC', 'SETTLEMENT', 'TRADED_QUA', 
                       'TRD_NO_CON', 'TRADED_VAL']
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Rename columns to match the SQLite table column names
    df.rename(columns={
        'CONTRACT_D': 'contract_d',
        'PREVIOUS_S': 'previous_s',
        'OPEN_PRICE': 'open_price',
        'HIGH_PRICE': 'high_price',
        'LOW_PRICE': 'low_price',
        'CLOSE_PRIC': 'close_price',
        'SETTLEMENT': 'settlement',
        'NET_CHANGE': 'net_change',
        'OI_NO_CON': 'oi_no_con',
        'TRADED_QUA': 'traded_qua',
        'TRD_NO_CON': 'trd_no_con',
        'TRADED_VAL': 'traded_val'
    }, inplace=True)
    
    return df

# Function to fetch unique dates from database
def get_unique_dates():
    conn = sqlite3.connect('bhavcopy.db')
    try:
        dates = pd.read_sql_query("SELECT DISTINCT date FROM bhavcopy ORDER BY date", conn)
        return dates['date'].tolist()
    except Exception as e:
        st.error(f"Error fetching dates: {e}")
        return []
    finally:
        conn.close()

# Function to perform comparison
def perform_comparison(date1, date2):
    conn = sqlite3.connect('bhavcopy.db')
    
    # Fetch data for both dates
    query = f"""
    SELECT 
        t1.contract_d, 
        t1.oi_no_con as oi_today, 
        t2.oi_no_con as oi_comparison,
        t1.close_price as close_today,
        t2.close_price as close_comparison
    FROM 
        bhavcopy t1
    LEFT JOIN 
        bhavcopy t2 ON t1.contract_d = t2.contract_d AND t2.date = '{date2}'
    WHERE 
        t1.date = '{date1}'
    """
    
    try:
        comparison_df = pd.read_sql_query(query, conn)
        
        # Remove rows where comparison data is missing
        comparison_df.dropna(subset=['oi_comparison', 'close_comparison'], inplace=True)
        
        # Calculate changes
        comparison_df['OI_Change'] = comparison_df['oi_today'] - comparison_df['oi_comparison']
        comparison_df['Price_Change'] = comparison_df['close_today'] - comparison_df['close_comparison']
        
        return comparison_df
    except Exception as e:
        st.error(f"Comparison error: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

# Streamlit app
def main():
    st.title("Bhavcopy Comparison Dashboard")
    
    # Initialize database
    init_db()
    
    # File uploader (multiple files)
    uploaded_files = st.file_uploader("Upload CSV Files", type="csv", accept_multiple_files=True)
    
    # Process uploaded files
    if uploaded_files:
        for file in uploaded_files:
            try:
                df = process_csv(file)
                insert_data(df)
            except Exception as e:
                st.error(f"Error processing {file.name}: {e}")
        st.success(f"Processed {len(uploaded_files)} files successfully!")
    
    # Get unique dates
    unique_dates = get_unique_dates()
    
    # Date selection
    if unique_dates:
        col1, col2 = st.columns(2)
        
        with col1:
            date1 = st.selectbox("Select First Date", unique_dates, index=len(unique_dates)-1)
        
        with col2:
            date2 = st.selectbox("Select Comparison Date", unique_dates)
        
        # Comparison button
        if date1 and date2 and date1 != date2:
            if st.button("Compare Dates"):
                # Perform comparison
                comparison_results = perform_comparison(date1, date2)
                
                if not comparison_results.empty:
                    # Display results
                    st.write(f"### Comparison between {date1} and {date2}")
                    
                    # Full comparison table
                    st.dataframe(comparison_results)
                    
                    # Visualizations
                    st.write("### Open Interest Changes")
                    st.bar_chart(comparison_results.set_index('contract_d')['OI_Change'])
                    
                    st.write("### Price Changes")
                    st.line_chart(comparison_results.set_index('contract_d')['Price_Change'])
                else:
                    st.warning("No comparable data found for the selected dates")

if __name__ == "__main__":
    main()