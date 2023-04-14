# tkg_active_sub.py - Get all inactive TGK subscriptions and create Rebundle + Recharge import files

# Imports #
import ast
from mig_funcs import get_recharge_data, get_last_order, get_meal_info, flag_alc_order
import pandas as pd

# Fetch all active subscriptions from Recharge #
inactive_subs = get_recharge_data(
    start_date='2020-01-15',
    end_date='2021-01-15',
    status='cancelled'
)

# Create df and keep only required columns #
df_inactive_subs = pd.json_normalize(inactive_subs)
df_inactive_subs = df_inactive_subs.loc[:, ['email', 'id', 'created_at', 'status', 'product_title', 'sku',
                                            'variant_title', 'shopify_variant_id']]
df_inactive_subs.columns = ['email', 'sub_id', 'created_at', 'status', 'sub_product_title', 'sub_sku',
                            'sub_variant_title', 'sub_variant_id']

# Set up columns to swap customers into Rebundle subs and default boxes
df_swap_table = pd.read_csv('sub_meal_swap.csv', converters={'meal_variant_ids': ast.literal_eval, 'meal_qtys': ast.literal_eval})
df_swap_table['new_variant_id'] = df_swap_table['new_variant_id'].astype('Int64')  # convert column type to nullable integers
df_swap_table['new_order_interval_frequency'] = df_swap_table['new_order_interval_frequency'].astype('Int64')  # convert column type to nullable integers
df_subs_swapped = pd.merge(df_inactive_subs, df_swap_table, on='sub_variant_id', how='left')
df_subs_swapped = df_subs_swapped[df_subs_swapped['new_sku'].notna()]  # drop records that didn't merge

# Save dfs before creating Recharge exports
df_inactive_subs.to_csv('tgk_inactive_subs.csv', index=False)
df_subs_swapped.to_csv('tgk_subs_swapped.csv', index=False)

# Create Recharge swap export
df_recharge_swap_inactive = df_subs_swapped.loc[:, ['sub_id', 'sub_product_title', 'sub_variant_title', 'sub_sku',
                                           'sub_variant_id', 'new_product_title', 'new_variant_title', 'new_sku',
                                           'new_variant_id', 'new_order_interval_frequency', 'new_order_interval_unit']]
df_recharge_swap_inactive.columns = ['Subscription ID', 'Product Title', 'Variant Title', 'SKU', 'Variant ID',
                            'NEW Product Title', 'NEW Variant Title', 'NEW SKU', 'NEW Variant ID',
                            'new_order_interval_frequency', 'new_order_interval_unit']
df_recharge_swap_inactive.to_csv('recharge_swap_inactive.csv', index=False)

# Explode df and Create Rebundle meal export #
df_explode = df_subs_swapped.explode(['meal_variant_ids', 'meal_qtys'])  # Un-nest meal_ids, qtys
df_rebundle_meals = df_explode.loc[:, ['sub_id', 'meal_variant_ids', 'meal_qtys']]  # Keep id, status, meal_variant_ids, meal_qtys
df_rebundle_meals.columns = ['Subscription ID', 'Variant ID', 'Qty']  # Rename columns
df_rebundle_meals.to_csv('rebundle_meals_inactive.csv', index=False)
