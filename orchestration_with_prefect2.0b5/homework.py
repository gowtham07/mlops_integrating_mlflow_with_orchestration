import pandas as pd

from sklearn.feature_extraction import DictVectorizer
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner
from prefect import get_run_logger
import logging 
import datetime
from datetime import date
from dateutil.relativedelta import relativedelta
import pickle
from prefect.deployments import DeploymentSpec
from prefect.orion.schemas.schedules import IntervalSchedule
from prefect.orion.schemas.schedules import CronSchedule
from prefect.flow_runners import SubprocessFlowRunner

@task
def read_data(path):
    df = pd.read_parquet(path)
    return df

@task
def prepare_features(df, categorical, train=True):
    log = get_run_logger()
    df['duration'] = df.dropOff_datetime - df.pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60
    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    mean_duration = df.duration.mean()
    if train:
        log.info(f"The mean duration of training is {mean_duration}")
        print(f"The mean duration of training is {mean_duration}")
    else:
        print(f"The mean duration of validation is {mean_duration}")
        log.info(f"The mean duration of validation is {mean_duration}")
    
    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    return df

@task
def train_model(df, categorical):
    log = get_run_logger()
    train_dicts = df[categorical].to_dict(orient='records')
    dv = DictVectorizer()
    X_train = dv.fit_transform(train_dicts) 
    y_train = df.duration.values
    log.info(f"The shape of X_train is {X_train.shape}")
    print(f"The shape of X_train is {X_train.shape}")
    print(f"The DictVectorizer has {len(dv.feature_names_)} features")
    log.info(f"The DictVectorizer has {len(dv.feature_names_)} features")

    lr = LinearRegression()
    lr.fit(X_train, y_train)
    y_pred = lr.predict(X_train)
    mse = mean_squared_error(y_train, y_pred, squared=False)
    print(f"The MSE of training is: {mse}")
    log.info(f"The MSE of training is: {mse}")

    return lr, dv

@task
def run_model(df, categorical, dv, lr):
    log = get_run_logger()
    val_dicts = df[categorical].to_dict(orient='records')
    X_val = dv.transform(val_dicts) 
    y_pred = lr.predict(X_val)
    y_val = df.duration.values

    mse = mean_squared_error(y_val, y_pred, squared=False)
    print(f"The MSE of validation is: {mse}")
    log.info(f"The MSE of validation is: {mse}")
    return
@task
def get_paths(date):
    if date == None:
       
       train= (date.today())- relativedelta(months=2)
       train_month = train.month
       train_year = train.year
       val = (date.today())- relativedelta(months=1)
       val_month = val.month
       val_year = val.year
       train_path = './data/fhv_tripdata_'+str(train_year)+'-'+str(train_month)+'.parquet'
       val_path = './data/fhv_tripdata_'+str(val_year)+'-'+str(val_month)+'.parquet'
    else:
       year,month,day = date.split("-")
       date = datetime.date(int(year),int(month),int(day))
       train= (date)- relativedelta(months=2)
       train_month = train.month
       train_month = str(train_month).zfill(2)
       train_year = train.year
       val = (date)- relativedelta(months=1)
       val_month = val.month
       val_month = str(val_month).zfill(2)
       val_year = val.year
       train_path = './data/fhv_tripdata_'+str(train_year)+'-'+str(train_month)+'.parquet'
       val_path = './data/fhv_tripdata_'+str(val_year)+'-'+str(val_month)+'.parquet'

    return   train_path,val_path

@flow(name="main entry",task_runner=SequentialTaskRunner())
def main(date = None):
    train_path, val_path = get_paths(date).result()
    log = get_run_logger()
    log.info(train_path)
    categorical = ['PUlocationID', 'DOlocationID']

    df_train = read_data(train_path)
    df_train_processed = prepare_features(df_train, categorical)

    df_val = read_data(val_path)
    df_val_processed = prepare_features(df_val, categorical, False)

    # train the model
    lr, dv = train_model(df_train_processed, categorical).result()
    run_model(df_val_processed, categorical, dv, lr)
    if date is None:
        date = datetime.today.strftime("%Y-%m-%d")
    with open(f'./models/dv-{date}.b', 'wb') as f_out:
        pickle.dump(dv, f_out)

main(date="2021-08-15")

deployment = DeploymentSpec(
    flow=main,
    name="example-deployment", 
    
    tags=["demo"],
    schedule=(CronSchedule(cron="0 9 5 * *", timezone="Europe/Berlin")),
    flow_runner=SubprocessFlowRunner(),

)




