import time

import pandas as pd
import numpy as np

from datetime import datetime
from settings import Settings
from pyspark.sql import SparkSession
from pyspark import SparkContext
from sklearn.preprocessing import MinMaxScaler
from joblib import dump, load
from keras.models import Sequential, load_model
from keras.layers import Dense, LSTM

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


def build_lstm_model(X_train, Y_train, X_test, Y_test, scaler, num_epochs, symbol_id=1):
    model = Sequential()
    model.add(LSTM(256, input_shape=(X_train.shape[1], 1)))
    model.add(Dense(1))
    model.compile(optimizer='adam', loss='mse')

    fitted_model = model.fit(x=X_train, y=Y_train, epochs=num_epochs, validation_data=(X_test, Y_test), shuffle=False)

    model_datetimestamp ="lstm_" + str(datetime.now().strftime("%Y%m%d_%H_%M"))
    model_name = "lstm_model" + model_datetimestamp + ".keras"
    scaler_name = "lstm_scaler" + model_datetimestamp + ".bin"

    #log model in database
    insert_model_to_db(model_name, 'Y', symbol_id)

    # store model and scaler to disk
    model.save('./models/' + model_name)
    dump(scaler, './models/' + scaler_name, compress=True)



def get_data(db_url, table_name, symbol_id=1, num_epochs=100):
    #TODO: migrate ml solution to spark and Docker
    #spark = build_spark()
    #table_name = "d_symbols"
    #df = spark.read.jdbc(db_url, table_name)
    #TODO: check why loading from database is not working/ too slow
    # engine = create_engine(db_url, echo=True)
    # with engine.connect() as connection:
    #     try:
    #         table = Table(table_name, MetaData(), autoload_with=engine)
    #         stmt = select(table).where(table.c.dvkpi_symbol_id == 1)
    #         result = connection.execute(stmt)
    #         df_data = pd.DataFrame(result)
    #     except Exception as e:
    #         print(f"Error retrieving KPI data: {e}")

    df_data = pd.read_csv("./datasets/f_dvkpi.csv", header=0)
    # Unpivot data
    df_data = df_data.pivot(index='dvkpi_timestamp', columns="dvkpi_kpi",values='dvkpi_kpi_value')
    split_percentage=0.9
    split_point = round(len(df_data) * split_percentage)

    train_data = pd.DataFrame(df_data.iloc[:split_point].iloc[:,0])
    test_data = pd.DataFrame(df_data.iloc[split_point:].iloc[:,0])

    scaler = MinMaxScaler()
    scaler.fit(train_data)
    scaled_train = scaler.transform(train_data)
    scaled_test = scaler.transform(test_data)
    X_train, Y_train, X_test, Y_test = timeseries_preprocessing(scaled_train, scaled_test, 10)

    # Build Model
    build_lstm_model(X_train, Y_train, X_test, Y_test, scaler, num_epochs, symbol_id)

    #Y_predicted=scaler.inverse_transform(model.predict(X_test))
    #Y_true = scaler.inverse_transform(Y_test.reshape(Y_test.shape[0], 1))
    #from sklearn import metrics
    #print('Model accuracy (%)')
    #Y_p=scaler.inverse_transform(model.predict(X_train))
    #Y_t=scaler.inverse_transform(Y_train.reshape(Y_train.shape[0],1))
    # print((1-(metrics.mean_absolute_error(Y_t, Y_p)/Y_t.mean()))*100)
    # print('')
    # print('Prediction performance')
    # print('MAE in %', (metrics.mean_absolute_error(Y_true, Y_predicted)/Y_true.mean())*100)
    # print('MSE', metrics.mean_squared_error(Y_true, Y_predicted))
    # print('RMSE',np.sqrt(metrics.mean_squared_error(Y_true, Y_predicted)))
    # print('R2', metrics.r2_score(Y_true, Y_predicted))
    #Y_predicted = scaler.inverse_transform(model.predict(X_test))
    #Y_true = scaler.inverse_transform(Y_test.reshape(Y_test.shape[0], 1))


def get_transformed_data(df_data, scaler, lags=10):
    scaled_data = scaler.transform(df_data)
    X = []
    for t in range(len(scaled_data) - lags - 1):
        X.append(scaled_data[t:(t + lags), 0])

    return X


def get_predicted_data(model_file_name, X, scaler_file_name):
    model = load_model("./models/" + model_file_name)
    scaler = load("./models/" + scaler_file_name)
    lags = model.input_shape[1]
    X.drop_duplicates(inplace=True)
    X = X.pivot(index='dvkpi_timestamp', columns="dvkpi_kpi", values='dvkpi_kpi_value')
    X_scaled = get_transformed_data(pd.DataFrame(X.iloc[:,0]), scaler, lags)
    Y_pred = model.predict(pd.DataFrame(X_scaled))
    Y_pred = scaler.inverse_transform(Y_pred)
    return Y_pred

#TODO: write actual function on investment decision
def make_investment_decision(Y_pred):
    return "BUY" if Y_pred[0] > 0 else "SELL"

def get_valid_model(db_url, symbol_id=1):
    engine = create_engine(db_url, echo=False)

    with engine.connect() as connection:
        try:
            tab_models = Table("d_models", MetaData(), autoload_with=engine)
            stmt = select(tab_models.c.model_filename).where(tab_models.c.model_active == 'Y' and tab_models.c.symbol_id == symbol_id)
            result = connection.execute(stmt)
            model_file_name = pd.DataFrame(result).values[0]
            return model_file_name
        except Exception as e:
            print(f"Error retrieving model from database: {e}")

def insert_model_to_db(model_file_name, model_active, symbol_id=1):
    db_url = Settings.get_setting("db_conn")
    engine = create_engine(db_url)

    with engine.connect() as connection:
        try:
            tab_models = Table("d_models", MetaData(), autoload_with=engine)
            stmt = select(tab_models.c.model_id)
            result = connection.execute(stmt)
            model_id = max(pd.Series(result)) + 1 if result is not None else 1
            ins = insert(tab_models).values(model_timestamp= datetime.now(),
                                       model_id = model_id,
                                       model_active=model_active,
                                       model_filename=model_file_name,
                                       symbol_id=symbol_id)
            connection.execute(ins)
        except Exception as e:
            print(f"Error inserting model to database: {e}")
