from spark import initialize_spark
from pyspark.sql.functions import *
from lesson4.examples.etl.loader import *


def main():
    """Entry point for the application script"""

    #load transaction data day1
    session = initialize_spark()
    transaction_path = "/training/das_2021/lesson4/examples/resources/transactions.csv"
    merchant_path = "/training/das_2021/lesson4/examples/resources/merchants.csv"
    transactions = load_from_csv(session,transaction_path)
    merchants =  load_from_csv(session,merchant_path)

    #clean data and remove duplicates
    clean = transactions\
        .dropDuplicates() \
        .filter('TRANSACTION_ID is not null and OUTLET_ID is not null and AMOUNT is not null') \
        .withColumn("PROCESS_DATE",current_date())

    merchants.show(truncate =False)
    clean.show(truncate =False)

    #enhance data
    joined = clean.join(merchants,'OUTLET_ID')
    joined \
        .show(truncate =False)

    #render total sales amount and total transactions per merchant
    sales =  joined.groupBy("MERCHANT_NAME") \
                 .agg(sum("AMOUNT").alias("TOTAL_AMOUNT"), count("TRANSACTION_ID").alias("TOTAL_NR"))

    sales.show()

    #persist data
    transaction_output = "/output/transaction/"
    sales_output = "/output/sales/"
    write_part_parquet(joined,transaction_output,"TRANSACTION_DATE")
    write_parquet(sales,sales_output)


if __name__ == '__main__':
    main()
