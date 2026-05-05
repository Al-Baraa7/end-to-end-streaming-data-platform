from faker import Faker
import uuid
import random
from datetime import datetime,timezone, timedelta

faker = Faker()
#--------------------------------------------------

# Users 

def generate_user_ids(n=1000):
    return [str(uuid.uuid4()) for _ in range(n)]

def generate_users(user_ids):

    users = []

    country_dict = {
        'KSA': 10,
        'UAE': 6,
        'EGY': 7,
        'KWT': 5,
        'QAT': 5,
        'MAR': 4,
        'JOR': 5
    }

    status_dict = {
        'active': 10,
        'suspended': 4,
        'deleted': 2
    }

    countries = list(country_dict.keys())
    country_weights = list(country_dict.values())

    statuses = list(status_dict.keys())
    status_weights = list(status_dict.values())

    for user_id in user_ids:

        
        email = faker.email() if random.random() > 0.03 else None # dirty data: 3% null emails

        country = random.choices(
            countries, weights=country_weights, k=1
        )[0]

        # dirty data: 2% unknown country
        if random.random() <0.02:
            country = "unknown"

        account_status = random.choices(
            statuses, weights=status_weights, k=1
        )[0]

        
        
        now = datetime.now(timezone.utc)
        created_at = now - timedelta(
            days=random.randint(0, 730),
            seconds=random.randint(0, 86399)
        )

        if account_status == 'active':
            last_login = created_at + timedelta(days=random.randint(0, 30))
        elif account_status == 'suspended':
            last_login = None if random.random() < 0.5 else created_at + timedelta(days=random.randint(0, 10))
        else:
            last_login = None
        
        # dirty data
        if account_status == "active" and random.random() < 0.05:
            last_login = created_at - timedelta(days=random.randint(1, 10))       
        

        users.append({
            "user_id": user_id,
            "email": email,
            "country": country,
            "account_status": account_status,
            "created_at": created_at,
            "last_login": last_login
        })

        
    return users


# ---------------------------------------------

# Data Fore Subscriptions

def generate_subscriptions(user_ids):
    data = []

    for user_id in user_ids:
        plan_type = random.choice(['free','premium'])

        start_date = faker.date_time_between(start_date='-1y',end_date='now',tzinfo=timezone.utc)

        if plan_type == 'free':
            end_date = None
            is_active = True
            auto_renew = False
        else:
            duration = random.choice([30,90,365])
            end_date = start_date + timedelta(days=duration)
            is_active = end_date > datetime.now(timezone.utc)
            auto_renew = faker.boolean()

        # dirty data
        if plan_type == 'premium' and random.random() < 0.05:
            end_date = start_date - timedelta(days=random.randint(1, 5))
            is_active = False

        data.append({
            'subscription_id': str(uuid.uuid4()),
            'user_id': user_id,
            'plan_type': plan_type,
            'start_date': start_date,
            'end_date': end_date if end_date else None,
            'is_active': is_active,
            'auto_renew': auto_renew
        })

        

    return data

   

def generate_payments(subs):
    payment = []

    for sub in subs:
        if sub["plan_type"] == 'free':
            continue

        amount = random.choice([9.99,14.99,29.99])
        currency = "USD"

        # dirty data
        if random.random() < 0.04:
            amount = -amount

        payment.append({
            "payment_id" : str(uuid.uuid4()),
            'subscription_id': sub["subscription_id"],
            "amount" : amount,
            "currency" : currency,
            "payment_date": sub["start_date"],
            "payment_status" : random.choices(
                ['success','failed','refunded'],
                weights=[85,10,5],
                k=1
            )[0]
        })
    return payment
#---------------------------------------

