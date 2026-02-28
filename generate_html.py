import sqlite3
import pandas as pd
from pathlib import Path

def generate_html():
    db_path = Path("data/belgian_macro.db")
    if not db_path.exists():
        print(f"Database {db_path} not found.")
        return

    conn = sqlite3.connect(db_path)
    
    # Query data
    df = pd.read_sql_query("""
        SELECT period, indicator_code, value 
        FROM observations 
        WHERE indicator_code IN (
            'GDP_ANNUAL_YY',
            'PRIV_CONSUMPTION_YY',
            'GOV_CONSUMPTION_YY',
            'GFCF_DWELLINGS_YY',
            'CHG_STOCKS_YY',
            'NET_EXPORTS_YY',
            'GFCF_ENTERPRISES_YY',
            'GFCF_PUBLIC_YY'
        )
        AND length(period) = 4
    """, conn)
    
    conn.close()

    # Pivot the dataframe to get years as rows and indicators as columns
    df_pivot = df.pivot(index='period', columns='indicator_code', values='value').reset_index()
    
    # Rename columns to match the image headers
    columns_mapping = {
        'period': 'Year',
        'GDP_ANNUAL_YY': 'GDP',
        'PRIV_CONSUMPTION_YY': 'Private final consumption',
        'GOV_CONSUMPTION_YY': 'Final consumption expenditure of general government',
        'GFCF_DWELLINGS_YY': 'fixed capital formation in dwelling',
        'CHG_STOCKS_YY': 'Change in stocks',
        'NET_EXPORTS_YY': 'Net exports',
        'GFCF_ENTERPRISES_YY': 'fixed capital formation by enterprises',
        'GFCF_PUBLIC_YY': 'Gross fixed capital formation by public administratio',
    }
    
    df_pivot = df_pivot.rename(columns=columns_mapping)
    
    # Calculate total investment (business + residential) if possible, but the image has "investment (business + residential)"
    # GFCF_ENTERPRISES_YY + GFCF_DWELLINGS_YY ?
    if 'fixed capital formation by enterprises' in df_pivot.columns and 'fixed capital formation in dwelling' in df_pivot.columns:
        df_pivot['investment (business + residential)'] = df_pivot['fixed capital formation by enterprises'] + df_pivot['fixed capital formation in dwelling']
    else:
        df_pivot['investment (business + residential)'] = None

    # Sort by year ascending
    df_pivot = df_pivot.sort_values('Year')

    # Convert to HTML
    html_template = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Belgian Macro Data</title>
        <style>
            table {
                border-collapse: collapse;
                width: 100%;
                font-family: Arial, sans-serif;
                font-size: 12px;
            }
            th, td {
                border: 1px solid #ddd;
                padding: 8px;
                text-align: right;
            }
            th {
                background-color: #4da6ff;
                color: white;
                text-align: center;
                vertical-align: bottom;
                white-space: pre-wrap;
            }
            th div {
                display: flex;
                align-items: center;
                justify-content: center;
            }
        </style>
    </head>
    <body>
        <h2>Belgian Macro Data</h2>
        <table>
            <thead>
                <tr>
                    <th>Year</th>
                    <th>GDP</th>
                    <th>Year</th>
                    <th>Private<br>final<br>consumpti<br>on</th>
                    <th>Year</th>
                    <th>Final<br>consumption<br>expenditure of<br>general<br>government</th>
                    <th>Year</th>
                    <th>fixed<br>capital<br>formation<br>in<br>dwelling</th>
                    <th>Year</th>
                    <th>Change<br>in stoc</th>
                    <th>Year</th>
                    <th>Net<br>export<br>s</th>
                    <th>Year</th>
                    <th>fixed<br>capital<br>formation<br>by<br>enterpris</th>
                    <th>Year</th>
                    <th>Gross fixed<br>capital<br>formation by<br>public<br>administratio</th>
                    <th>investment<br>(business<br>+<br>residen</th>
                </tr>
            </thead>
            <tbody>
                {rows}
            </tbody>
        </table>
    </body>
    </html>
    """

    rows_html = ""
    for _, row in df_pivot.iterrows():
        year = str(row['Year'])
        rows_html += f"""
        <tr>
            <td>{year}</td><td>{row.get('GDP', '')}</td>
            <td>{year}</td><td>{row.get('Private final consumption', '')}</td>
            <td>{year}</td><td>{row.get('Final consumption expenditure of general government', '')}</td>
            <td>{year}</td><td>{row.get('fixed capital formation in dwelling', '')}</td>
            <td>{year}</td><td>{row.get('Change in stocks', '')}</td>
            <td>{year}</td><td>{row.get('Net exports', '')}</td>
            <td>{year}</td><td>{row.get('fixed capital formation by enterprises', '')}</td>
            <td>{year}</td><td>{row.get('Gross fixed capital formation by public administratio', '')}</td>
            <td>{row.get('investment (business + residential)', '')}</td>
        </tr>
        """
        
    final_html = html_template.replace('{rows}', rows_html)
    
    with open('data/belgian_macro.html', 'w') as f:
        f.write(final_html)
        
    print("HTML table generated at data/belgian_macro.html")

if __name__ == "__main__":
    generate_html()
