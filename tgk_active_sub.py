# tkg_active_sub.py - Get all active TGK subscriptions and create Rebundle + Recharge import files

# Imports #
from mig_funcs import get_recharge_data, get_last_order, get_meal_info, flag_alc_order
import pandas as pd

# Fetch all active subscriptions from Recharge #
active_subs = get_recharge_data(
    start_date='2022-03-08',
    end_date='2022-03-10',
    status='active'
)

# Create df and keep only required columns #
df_active_subs = pd.json_normalize(active_subs)
df_active_subs = df_active_subs.loc[:, ['email', 'id', 'created_at','status', 'product_title', 'sku', 'variant_title',
                                      'shopify_variant_id']]
df_active_subs.columns = ['email', 'sub_id', 'created_at','status', 'sub_product_title', 'sub_sku', 'sub_variant_title',
                          'sub_variant_id']

# Look up most recent Shopify order id #
df_active_subs['last_order_id'] = df_active_subs.apply( # Create df column with last order id
    lambda row:
        get_last_order(row['email']),
        axis=1
)

# Get meal info #
df_active_subs[['meal_skus', 'meal_product_ids', 'meal_variant_ids', 'meal_qtys']] = df_active_subs.apply(
    lambda row:
        get_meal_info(row['last_order_id']),
    axis=1,
    result_type='expand'
)

# Flag orders with ALC or sample packs and remove them to a separate df #
df_active_subs['alc_order'] = df_active_subs.apply(
    lambda row:
        flag_alc_order(row['meal_skus']),
        axis=1
)
df_alc_subs = df_active_subs[df_active_subs['alc_order'] == True]
df_alc_subs.to_csv('alc_subs.csv', index=False)
df_active_subs = df_active_subs[~df_active_subs['alc_order'] == True]

# Set up columns to swap customers into Rebundle subs
df_swap_table = pd.read_csv('sub_swap_final.csv')
df_swap_table['new_variant_id'] = df_swap_table['new_variant_id'].astype('Int64')  # convert column to nullable integers
df_swap_table['new_order_interval_frequency'] = df_swap_table['new_order_interval_frequency'].astype('Int64')  # convert column to nullable integers
df_subs_swapped = pd.merge(df_active_subs, df_swap_table, on='sub_variant_id', how='left')
df_subs_swapped = df_subs_swapped[df_subs_swapped['new_sku'].notna()]  # drop records that didn't merge

# Save dfs before creating Recharge exports
df_active_subs.to_csv('tgk_active_subs.csv', index=False)


# Create Recharge swap export
df_recharge_swap = df_subs_swapped.loc[:, ['sub_id', 'sub_product_title', 'sub_variant_title', 'sub_sku',
                                           'sub_variant_id', 'new_product_title', 'new_variant_title', 'new_sku',
                                           'new_variant_id', 'new_order_interval_frequency', 'new_order_interval_unit']]
df_recharge_swap.columns = ['Subscription ID', 'Product Title', 'Variant Title', 'SKU', 'Variant ID',
                            'NEW Product Title', 'NEW Variant Title', 'NEW SKU', 'NEW Variant ID',
                            'new_order_interval_frequency', 'new_order_interval_unit']
df_recharge_swap.to_csv('recharge_swap.csv', index=False)

# Verify customer meal count matches subscription count #
# create column with meal count of last order
df_subs_swapped['last_order_count'] = df_subs_swapped.apply(
    lambda row:
        sum(row['meal_qtys']),
        axis=1
)
# if last order count equals new_variant count, new_meal_variant_ids and new_variant_qtys are same


df_subs_swapped.to_csv('tgk_subs_swapped.csv', index=False)