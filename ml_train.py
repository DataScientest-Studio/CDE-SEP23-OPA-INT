import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark import SparkContext
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, LSTM
from pyspark.sql import Row
from sqlalchemy import create_engine, Table, Column, Integer, Numeric, String, Boolean, MetaData, TIMESTAMP, inspect, \
    insert, select, text, PrimaryKeyConstraint, ForeignKey

def build_spark():
    sc = SparkContext.getOrCreate()
    # Building a Spark Session
    spark = SparkSession \
        .builder \
        .appName("Crypto Bot") \
        .config("spark.driver.extraClassPath", "./postgresql-42.5.1.jar") \
        .getOrCreate()
    return spark
#.config("spark.jars", "./postgresql-42.7.1.jar") \

def timeseries_preprocessing(scaled_train, scaled_test, lags):
    X, Y = [], []
    for t in range(len(scaled_train) - lags - 1):
        X.append(scaled_train[t:(t + lags), 0])
        Y.append(scaled_train[(t + lags), 0])

    Z, W = [], []
    for t in range(len(scaled_test) - lags - 1):
        Z.append(scaled_test[t:(t + lags), 0])
        W.append(scaled_test[(t + lags), 0])

    X_train, Y_train, X_test, Y_test = np.array(X), np.array(Y), np.array(Z), np.array(W)

    X_train = X_train.reshape((X_train.shape[0], X_train.shape[1], 1))
    X_test = X_test.reshape((X_test.shape[0], X_test.shape[1], 1))

    return X_train, Y_train, X_test, Y_test





def get_data(db_url, table_name):
    #TODO: migrate ml solution to spark and Docker
    #spark = build_spark()
    #table_name = "d_symbols"
    #df = spark.read.jdbc(db_url, table_name)
    # engine = create_engine(db_url, echo=True)
    #
    # with engine.connect() as connection:
    #     try:
    #         table = Table(table_name, MetaData(), autoload_with=engine)
    #         stmt = select(table).where(table.c.dvkpi_symbol_id == 1)
    #         result = connection.execute(stmt)
    #         df_data = pd.DataFrame(result)
    #     except Exception as e:
    #         print(f"Error retrieving KPI data: {e}")

    df_data = pd.read_csv("f_dvkpi.csv", header=0)
    # Unpivot data
    df_data = df_data.pivot(index='dvkpi_timestamp', columns="dvkpi_kpi",values='dvkpi_kpi_value')
    split_percentage=0.9
    split_point = round(len(df_data) * split_percentage)

    train_data = df_data.iloc[:split_point]
    test_data = df_data.iloc[split_point:]

    scaler = MinMaxScaler()
    scaler.fit(train_data)
    scaled_train = scaler.transform(train_data)
    scaled_test = scaler.transform(test_data)
    X_train, Y_train, X_test, Y_test = timeseries_preprocessing(scaled_train, scaled_test, 10)

    model = Sequential()
    model.add(LSTM(256, input_shape=(X_train.shape[1], 1)))
    model.add(Dense(1))
    model.compile(optimizer='adam', loss='mse')

    history = model.fit(x=X_train, y=Y_train, epochs=300, validation_data=(X_test, Y_test), shuffle=False)


