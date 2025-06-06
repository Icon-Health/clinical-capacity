# clinical-capacity

python script will execute the following:

1. run referral_count_arima_model.sql to create model needed for forces
2. generate a forecast for 16 weeks out referral_count_arima_model_forecast.sql
3. join actuals and forecast in referral_count_arima_model_final.sql
3. generate conversion rate 8 week moving average
