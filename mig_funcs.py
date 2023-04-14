# Function for migration

# Imports #
from dotenv import load_dotenv
import os
import requests
import time

# Load .env variables #
load_dotenv()
recharge_api_token = os.getenv("RECHARGE_API_TOKEN")
recharge_api_token_tb12 = os.getenv("RECHARGE_API_TOKEN_TB12")
shopify_access_token = os.getenv("SHOPIFY_ACCESS_TOKEN")

# Establish requests Sessions #
shopify_session = requests.Session()
recharge_session = requests.Session()


def get_recharge_data(store, start_date, end_date, status):
    """Fetch subscriptions from Recharge"""
    # Set endpoint variables
    if store == 'tgk':
        recharge_access_token = recharge_api_token
    if store == 'tb12':
        recharge_access_token = recharge_api_token_tb12
    headers = {"X-Recharge-Access-Token": recharge_access_token}
    payload = dict(limit='250', created_at_min=start_date, created_at_max=end_date, status=status)
    url = f"https://api.rechargeapps.com/subscriptions"
    # Access and store first page of responses
    response = recharge_session.get(url, headers=headers, params=payload)
    response_data = response.json()
    all_records = response_data['subscriptions']
    print('Get Page 1')
    # While Next Link is present, access and store next page
    count = 2
    while "next" in response.links:
        next_url = response.links["next"]["url"]
        response = recharge_session.get(next_url, headers=headers)
        response_data = response.json()
        all_records.extend(response_data['subscriptions'])
        print(f'Get Page {count}')
        count += 1
        # Sleep to avoid rate limit if approach threshold
        if response.headers['X-Recharge-Limit'] == '39/40':
            time.sleep(0.5)
    return all_records


def get_recharge_data_2(store, start_date, end_date):
    """Fetch subscriptions from Recharge"""
    # Set endpoint variables
    if store == 'tgk':
        recharge_access_token = recharge_api_token
    if store == 'tb12':
        recharge_access_token = recharge_api_token_tb12
    headers = {"X-Recharge-Access-Token": recharge_access_token}
    payload = dict(limit='250', created_at_min=start_date, created_at_max=end_date)
    url = f"https://api.rechargeapps.com/subscriptions"
    # Access and store first page of responses
    response = recharge_session.get(url, headers=headers, params=payload)
    response_data = response.json()
    all_records = response_data['subscriptions']
    print('Get Page 1')
    # While Next Link is present, access and store next page
    count = 2
    while "next" in response.links:
        next_url = response.links["next"]["url"]
        response = recharge_session.get(next_url, headers=headers)
        response_data = response.json()
        all_records.extend(response_data['subscriptions'])
        print(f'Get Page {count}')
        count += 1
        # Sleep to avoid rate limit if approach threshold
        if response.headers['X-Recharge-Limit'] == '39/40':
            time.sleep(0.5)
    return all_records


def get_all_records(resource):
    """Get all records for a given resource in tgk recharge

    Keyword arguments:
        resource -- addresses,
    """
    # Set endpoint variables
    headers = {'X-Recharge-Access-Token': recharge_api_token}
    payload = dict(limit='250')
    url = f'https://api.rechargeapps.com/{resource}'
    # Access and store first page of responses
    response = recharge_session.get(url, headers=headers, params=payload)
    response_data = response.json()
    all_records = response_data[resource]
    print(f'Get Page 1 of {resource}')
    # While Next Link is present, access and store next page
    count = 2
    while "next" in response.links:
        next_url = response.links["next"]["url"]
        response = recharge_session.get(next_url, headers=headers)
        response_data = response.json()
        all_records.extend(response_data[resource])
        print(f'Get Page {count} of {resource}')
        count += 1
        # Sleep to avoid rate limit if approach threshold
        if response.headers['X-Recharge-Limit'] == '39/40':
            time.sleep(0.5)
    return all_records


def get_last_order(email):
    """Look up most recent Shopify order id"""
    shop = "the-good-kitchen-esc.myshopify.com"
    headers = {"X-Shopify-Access-Token": shopify_access_token, "Content-Type": "application/json"}
    fields = 'email,last_order_id'
    url = f"https://{shop}/admin/api/2021-04/customers/search.json?query=email:{email}&fields={fields}"
    response = shopify_session.get(url, headers=headers)
    try:
        last_order_id = response.json()['customers'][0]['last_order_id']
    except KeyError:
        last_order_id = 0
    print(f'Order #{last_order_id}')
    return last_order_id


def get_meal_info(order_id):
    """Get meal skus, product ids, variant ids, and qtys for a Shopify order"""
    # TODO: Add meal name
    try:
        shop = "the-good-kitchen-esc.myshopify.com"
        headers = {"X-Shopify-Access-Token": shopify_access_token, "Content-Type": "application/json"}
        fields = 'email,line_items'
        url = f"https://{shop}/admin/api/2021-07/orders/{order_id}.json?fields={fields}"
        response = shopify_session.get(url, headers=headers)
        meal_skus = []
        meal_product_ids = []
        meal_variant_ids = []
        meal_qtys = []
        for line_item in response.json()['order']['line_items']:
            sku = line_item['sku']
            product_id = line_item['product_id']
            variant_id = line_item['variant_id']
            qty = line_item['quantity']
            if 'TGK' in sku:
                pass
            elif 'DTH' in sku:
                pass
            else:
                meal_skus.append(sku)
                meal_product_ids.append(product_id)
                meal_variant_ids.append(variant_id)
                meal_qtys.append(qty)
    except KeyError:
        meal_skus = []
        meal_product_ids = []
        meal_variant_ids = []
        meal_qtys = []

    print(meal_skus, meal_product_ids, meal_variant_ids, meal_qtys)
    return meal_skus, meal_product_ids, meal_variant_ids, meal_qtys


def flag_alc_order(meal_skus):
    """Flag orders w/ ALC products or sample packs"""
    try:
        if (any('ALA' in sku for sku in meal_skus)
            or any('ALC' in sku for sku in meal_skus)
            or any('SPL' in sku for sku in meal_skus)
        ):
            return True
        else:
            return False
    except TypeError:
        return True


def get_recharge_orders(limit, status, created_at_min, created_at_max):
    ''' Get orders from Recharge API

    Keyword arguments:
    limit - The amount of results. Maximum is 250, default is 50.
    status - Filter orders by status. Available status:
             “SUCCESS”, “QUEUED”, “ERROR”, “REFUNDED”, “SKIPPED”
    '''
    headers = {"X-Recharge-Access-Token": recharge_api_token}
    payload = dict(limit=limit, status=status, created_at_min=created_at_min, created_at_max=created_at_max)
    url = f"https://api.rechargeapps.com/orders"

    response = requests.get(url, headers=headers, params=payload)
    charge_data = response.json()['orders']
    return charge_data

def update_canc_reason(sub_id, new_reason):
    """Update a subscription's cancellation reason"""
    headers = {
        "X-Recharge-Version": "2021-11",
        "Content-Type": "application/json",
        "X-Recharge-Access-Token": recharge_access_token
    }
    data = {'cancellation_reason': new_reason}
    url = f'https://api.rechargeapps.com/subscriptions/{sub_id}'
    response = requests.put(url, data, headers=headers)