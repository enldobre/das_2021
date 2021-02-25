from lesson4.examples.etl.spark import initialize_spark


def main():
    """Entry point for the application script"""
    session = initialize_spark()
    transaction =  session.read.option("header","true").format('csv').load('transactions.csv')
    transaction.show()


if __name__ == '__main__':
    main()
