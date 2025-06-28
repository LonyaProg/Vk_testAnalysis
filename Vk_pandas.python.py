import pandas as pd
import numpy as np

np.random.seed(42)

n_rows = 100

user_ids = np.arange(1, 31)

event_names = ['launch', 'update']

times = pd.to_datetime(
    np.random.choice(
        pd.date_range(start='2024-02-01', end='2024-05-15', freq='H'),
        size=n_rows
    )
)

df = pd.DataFrame({
    'userId': np.random.choice(user_ids, size=n_rows),
    'eventName': np.random.choice(event_names, size=n_rows),
    'time': times
})

df['time'] = pd.to_datetime(df['time'])

launches = df[
    (df['eventName'] == 'launch') &
    (df['time'] >= '2024-03-01')
]

first_launch = launches.groupby('userId', as_index = False)['time'].min().rename(columns = {'time': 'ft'}).copy()
first_launch['week'] = first_launch['ft'].dt.to_period('W-MON').dt.start_time.dt.date
merged = first_launch.merge(df, on = 'userId', how = 'left')

updates = merged[
    (merged['eventName'] == 'update') &
    (merged['time'] >= merged['ft']) &
    (merged['time'] <= merged['ft'] + pd.Timedelta(days=14))
][['userId', 'week']].drop_duplicates()

launch_count = (
    first_launch
    .groupby('week')
    .agg(users = ('userId', 'nunique'))
    .reset_index()
)

updated_count = (
    updates
    .groupby('week')
    .agg(updated_users = ('userId', 'nunique'))
    .reset_index()
)

resault = (
    launch_count.
    merge(updated_count, on = 'week', how = 'left')
    .dropna()
)

resault['CR'] = (resault['updated_users'] / resault['users'] * 100).round(2).astype(str) + '%'

result = resault[['week', 'users', 'CR']].sort_values('week')

print(resault)