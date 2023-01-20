import csd as c
import pandas as pd

# Buckets by which to categorize customer/item sales (how much a customer buys of an item in the timeframe)
# used in 05_summarize_data, 07_categorize_products notebooks, and then to identify incidental item cutoff
CUST_ITEM_BUCKETS = [0, 250, 500, 1000, 1500, 3000, 5000, 10000000]
CUST_ITEM_BUCKET_LABELS = ['<$250', '$250 to 500', '$500 to 1,000', '$1,000 to 1,500', '$1,500 to 3,000', '$3,000 to 5,000', '>$5,000']

# Cutoff for incidental items. Choose one of the items in CUST_ITEM_BUCKET_LABELS above. 
# e.g. if 
INCIDENTAL_CUTOFF_LABEL = '$1,000 to 1,500'

def pull_raw_data(start_date, end_date):
    query = f"""
        SELECT DISTINCT
            t1.whse,
            t1.orderno,
            t1.ordersuf,
            t1.lineno,
            t1.invoicedt,
            INT(t1.custno) custno,
            t2.name custname,
            t1.shipprod item,
            t3.descrip_1 || ' ' || t3.descrip_2 itemdesc,
            t1.unit,
            t1.unitconv,
            CASE
                WHEN t1.returnfl = 1 THEN t1.qtyship * -1
                ELSE t1.qtyship
            END units,
            t1.prodcost unitcost,
            t1.price unitprice,
            CASE
                WHEN t1.returnfl = 1 THEN t1.netamt * -1
                ELSE t1.netamt
            END netamt,
            t1.returnfl,
            UPPER(t1.transtype) transtype,
            CASE
                WHEN UPPER(t1.specnstype) = 'N' THEN 'nonstock'
                WHEN UPPER(t1.specnstype) = 'S' THEN 'special order'
                WHEN UPPER(t1.specnstype) = '' THEN 'stocked'
                ELSE 'other'
            END prod_type,
            NVL(t3.prodcat, 0) prodcat,
            t4.descrip cat_descrip,
            t1.rowpointer,
            t1.priceorigcd,
            t1.pdrecno,
            t1.vendno

        FROM oeel t1

        LEFT JOIN arsc t2
            ON t2.cono = t1.cono
            AND t2.custno = t1.custno

        LEFT JOIN icsp t3
            ON t3.cono = t1.cono
            AND t3.prod = t1.shipprod
            
        LEFT JOIN sasta t4
            ON t4.cono = t1.cono
            AND t4.codeval = t3.prodcat
            AND t4.codeiden = 'C'

        WHERE
            t1.cono = 1
            AND t1.invoicedt BETWEEN '{start_date}' AND '{end_date}'
            
        ORDER BY
            t1.orderno,
            t1.ordersuf,
            t1.lineno

    """
    
    output_df = c.run_query(query)
        
    output_df['xcost_adj'] = output_df['units'] * output_df['unitcost']
    output_df['GP$'] = (output_df['unitprice'] - output_df['unitcost']) * output_df['units']
    output_df['Margin'] = output_df['GP$'] / output_df['netamt']
           
    output_df['month'] = pd.to_datetime(output_df['invoicedt']).dt.strftime('%Y-%m')
    
    return output_df