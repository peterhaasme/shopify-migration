# create_recharge_backup.py
# create backup of all EatTGK recharge databases

# Imports #
from mig_funcs import get_all_records
import pandas as pd

# List of resources


resources = ['customers', 'discounts', 'onetimes',
             'orders', 'payment_methods', 'products', 'store', 'subscriptions']

resources = ['subscriptions']

for resource in resources:
    records = get_all_records(resource=resource)
    df = pd.json_normalize(records)
    df.to_csv(f'TGK_recharge_{resource}.csv', index=False)