import pandas as pd

df = pd.read_csv('nasdaq_boolean.csv',index_col='date',parse_dates=['date'],date_format='%Y-%m-%d')

df.head()

df['next_day_actual'].unique()

df.dtypes

prediction_df = df[['streak_len', 'direction', 'occurrence', 'performance', 'vol', 'ma5_pos', 'ma200_pos', 'ma5_neg', 'ma200_neg', 'next_day_actual']]
prediction_df

prediction_df = prediction_df.sort_index()
prediction_df.dropna(inplace=True)
prediction_df.head()

from sklearn.model_selection import train_test_split
x = prediction_df[['streak_len', 'direction', 'occurrence', 'performance', 'vol', 'ma5_pos', 'ma200_pos', 'ma5_neg', 'ma200_neg']]
y = prediction_df['next_day_actual']
x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=.2, random_state=42)

from sklearn.ensemble import RandomForestClassifier

model = RandomForestClassifier(n_estimators=100, random_state=42) # n_estimator is used to build decision trees(100 in this case)
model.fit(x_train, y_train)

from sklearn.metrics import classification_report
y_pred = model.predict(x_test)
print(classification_report(y_pred, y_test))

result_df = pd.DataFrame({
    'predicted_val': y_pred,
    'real_val': y_test,
    'truth_val': y_pred * y_test
})
result_df

result_df['truth_val'].value_counts(normalize=True)