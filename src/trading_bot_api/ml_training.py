import pandas as pd
import numpy as np
import os
os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'

from datetime import datetime
import scikeras.wrappers
from sklearn.preprocessing import StandardScaler
from joblib import dump, load
from keras.models import Sequential, load_model
from keras.layers import Dense, LSTM, Dropout

from sqlalchemy import create_engine, Table, MetaData, insert, select, update
from cross_cutting.db_driver import create_derived_kpis
import api_settings

lags_considered = 30


def build_model():
    lstm_model = Sequential()
    lstm_model.add(LSTM(50, return_sequences=True, input_shape=(lags_considered, 10)))
    lstm_model.add(Dropout(0.2))
    lstm_model.add(Dense(1))
    lstm_model.compile(loss='mse', optimizer='adam')
    return lstm_model


def createXY(dataset, n_past):
    dataX = []
    dataY = []
    for i in range(n_past, len(dataset)):
        # select past data of size n_past until but excluding i
        dataX.append(dataset[i - n_past:i, 0:dataset.shape[1]])
        # select the target variable but note that compared to dataX the index is shifted by 1
        dataY.append(dataset[i, 0])
    return np.array(dataX), np.array(dataY)


def estimate_new_model(db_url, table_name, symbol_id=1, num_epochs=30):
    df_data = pd.read_csv("./datasets/f_dvkpi.csv", header=0)
    if df_data is not None:
        df_data.sort_values(by='dvkpi_timestamp', ascending=True, inplace=True)
    # Unpivot data
    df_data = df_data.pivot(index='dvkpi_timestamp', columns="dvkpi_kpi", values='dvkpi_kpi_value')
    # Remove NULL values
    df_data.dropna(axis=0, how="any", inplace=True)
    df_data = manipulate_data(df_data)
    df_data.dropna(axis=0, how="any", inplace=True)
    split_percentage = 0.85
    split_point = round(len(df_data) * split_percentage)

    # FI was removed during project times
    try:
        df_data.drop("FI_20", axis=1, inplace=True)
        df_data.drop("FI_50", axis=1, inplace=True)
        df_data.drop("FI_100", axis=1, inplace=True)
    except:
        pass

    # Split Dataset
    train_data = pd.DataFrame(df_data.iloc[:split_point])
    test_data = pd.DataFrame(df_data.iloc[split_point:])

    # Scale Data
    scaler = StandardScaler()
    scaled_train = scaler.fit_transform(train_data)
    scaled_test = scaler.transform(test_data)

    # Crate train and test data as numpy arrays
    trainX, trainY = createXY(scaled_train, lags_considered)
    testX, testY = createXY(scaled_test, lags_considered)

    # Setup Keras Regressor
    keras_reg = scikeras.wrappers.KerasRegressor(build_fn=build_model)
    keras_reg.fit(trainX, trainY, epochs=num_epochs,
                  batch_size=64, verbose=1,
                  validation_data=(testX, testY),
                  shuffle=False)

    model_datetimestamp = str(datetime.now().strftime("%Y%m%d_%H_%M"))
    model_name = "lstm_model_" + model_datetimestamp + ".keras"
    scaler_name = "lstm_scaler_" + model_datetimestamp + ".bin"

    # store model and scaler to disk
    keras_reg.model_.save('./models/' + model_name)
    dump(scaler, './models/' + scaler_name, compress=True)

    # log model in database
    insert_model_to_db(model_name, 'Y', symbol_id)


def get_transformed_data(df_data, scaler):
    # FI was removed during project times
    try:
        df_data.drop("FI_20", axis=1, inplace=True)
        df_data.drop("FI_50", axis=1, inplace=True)
        df_data.drop("FI_100", axis=1, inplace=True)
    except:
        pass
    df_data = manipulate_data(df_data)
    scaled_data = scaler.transform(df_data)
    return scaled_data


def load_model_file(model_file_name):
    return load_model("./models/" + model_file_name)


def load_scaler_file(scaler_file_name):
    return load("./models/" + scaler_file_name)


def manipulate_data(X):
    l_columns_ma = ["MA_50", "MA_200", "MA_250", "EWMA_50", "EWMA_200", "EWMA_250"]
    for column in l_columns_ma:
        # Replace the columns ewma_200 and ma_50 with their relative difference to the value in avg_price
        X[column] = (X[column] - X['AVG_PRICE']) / X['AVG_PRICE']
    # Create first differences for column "AVG_PRICE"
    X['AVG_PRICE'] = X['AVG_PRICE'].diff()
    return X


def get_predicted_data(model, scaler, X, symbol_id, holding_period=10):
    y_actual = X[X["dvkpi_kpi"] == "AVG_PRICE"].iloc[:, [0, 2]].set_index("dvkpi_timestamp")
    y_actual = y_actual["dvkpi_kpi_value"]

    for i in range(0, holding_period):
        if i > 0:
            X = create_derived_kpis(y_actual, symbol_id, create_from_predictions=True)

        # Data Cleansing and Scaling
        X.drop_duplicates(inplace=True)
        X.dropna(axis=0, how="any", inplace=True)
        X = X.pivot(index='dvkpi_timestamp', columns="dvkpi_kpi", values='dvkpi_kpi_value')
        X_scaled = get_transformed_data(X, scaler)

        # Store dependent variable (note this is either the actually observed variable or a prediction)
        y = X.iloc[:, 1]

        # Reshape for LSTM
        X_scaled, Y_scaled = createXY(X_scaled, lags_considered)

        # Make prediction
        pred_scaled = model.predict(X_scaled, verbose=0)[-1]
        # Make transformations so that shapes match
        pred_scaled_repeated = np.repeat(pred_scaled, 10, axis=-1)
        # Prediction for the next minute as difference
        pred_scaled_reshaped_inv = scaler.inverse_transform(np.reshape(pred_scaled_repeated,
                                                                       (len(pred_scaled), 10)))[:,0]
        # now add the last actual value to the prediction to get actual price
        pred_scaled_reshaped_inv = y_actual[-1] + pred_scaled_reshaped_inv[-1]
        # Add datetime for prediction (next minute since we use 1min klines)
        pred_y_datetime = (pd.to_datetime(y_actual.index[-1], format='%Y-%m-%d %H:%M:%S', utc=True)
                           + pd.DateOffset(minutes=1))
        # Concat and start over
        y_actual = pd.concat([y_actual, pd.Series(pred_scaled_reshaped_inv).set_axis([pred_y_datetime])])
    return y_actual.iloc[-1]


def make_investment_decision(y_pred, current_price):
    return "BUY" if y_pred > current_price else "SELL"


def get_valid_model(db_url, symbol_id=1):
    engine = create_engine(db_url, echo=False)

    with engine.connect() as connection:
        try:
            tab_models = Table("d_models", MetaData(), autoload_with=engine)
            stmt = select(tab_models.c.model_filename).where(
                tab_models.c.model_active == 'Y' and tab_models.c.symbol_id == symbol_id)
            result = connection.execute(stmt)
            model_file_name = pd.DataFrame(result).values[0]
            return model_file_name
        except Exception as e:
            print(f"Error retrieving model from database: {e}")


def insert_model_to_db(model_file_name, model_active, symbol_id=1):
    db_url = api_settings.db_conn
    engine = create_engine(db_url)

    with engine.connect() as connection:
        trans = connection.begin()
        try:
            tab_models = Table("d_models", MetaData(), autoload_with=engine)
            stmt = select(tab_models.c.model_id)
            result = connection.execute(stmt)
            model_id = int(max(pd.DataFrame(result).values)) + 1 if result is not None else 1

            if model_active == 'Y':
                # set all other models to inactive
                stmt = update(tab_models).where(tab_models.c.symbol_id == int(symbol_id)).values(model_active='N')
                connection.execute(stmt)

            ins = insert(tab_models).values(model_timestamp=datetime.now(),
                                            model_id=model_id,
                                            model_active=model_active,
                                            model_filename=model_file_name,
                                            symbol_id=int(symbol_id))
            connection.execute(ins)
            trans.commit()

        except Exception as e:
            print(f"Error inserting model to database: {e}")
