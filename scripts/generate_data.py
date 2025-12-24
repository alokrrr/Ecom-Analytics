from faker import Faker
import random, csv
from datetime import datetime
fake = Faker()
Faker.seed(42)
random.seed(42)

NUM_USERS = 5000
NUM_PRODUCTS = 500
NUM_ORDERS = 20000

# Users
with open("data/users.csv", "w", newline='', encoding='utf-8') as f:
    w = csv.writer(f)
    w.writerow(["user_id","email","signup_date","country","user_type"])
    for i in range(1, NUM_USERS+1):
        signup = fake.date_between(start_date='-3y', end_date='today')
        w.writerow([i, f"user{i}@example.com", signup.isoformat(), fake.country(), random.choice(["regular","vip"])])

# Products
categories = ["Shoes","Apparel","Electronics","Home","Beauty","Sports"]
with open("data/products.csv","w",newline='',encoding='utf-8') as f:
    w=csv.writer(f)
    w.writerow(["product_id","sku","name","description","category","price"])
    for i in range(1, NUM_PRODUCTS+1):
        name = fake.word().capitalize() + " " + random.choice(["Pro","X","Plus","Lite","Max"])
        desc = fake.sentence(nb_words=12)
        price = round(random.uniform(5, 500),2)
        w.writerow([i, f"SKU-{i:05d}", name, desc, random.choice(categories), price])

# Orders & Order Items
with open("data/orders.csv","w",newline='',encoding='utf-8') as fo, open("data/order_items.csv","w",newline='',encoding='utf-8') as fi:
    wo=csv.writer(fo); wi=csv.writer(fi)
    wo.writerow(["order_id","user_id","order_date","status","total_amount"])
    wi.writerow(["order_item_id","order_id","product_id","quantity","unit_price"])
    order_item_id = 1
    for oid in range(1, NUM_ORDERS+1):
        uid = random.randint(1, NUM_USERS)
        date = fake.date_time_between(start_date='-2y', end_date='now')
        status = random.choices(["completed","cancelled","returned"], weights=[0.9,0.07,0.03])[0]
        num_items = random.choices([1,2,3,4], weights=[0.6,0.25,0.1,0.05])[0]
        total = 0
        for _ in range(num_items):
            pid = random.randint(1, NUM_PRODUCTS)
            qty = random.choices([1,2,3], weights=[0.8,0.15,0.05])[0]
            price = round(random.uniform(5, 500),2)
            total += price * qty
            wi.writerow([order_item_id, oid, pid, qty, price])
            order_item_id += 1
        wo.writerow([oid, uid, date.isoformat(), status, round(total,2)])

# Reviews
with open("data/reviews.csv","w",newline='',encoding='utf-8') as f:
    w=csv.writer(f)
    w.writerow(["review_id","product_id","user_id","rating","review_text","review_date"])
    rid=1
    for _ in range(5000):
        pid=random.randint(1,NUM_PRODUCTS)
        uid=random.randint(1,NUM_USERS)
        rating=random.randint(1,5)
        text=fake.sentence(nb_words=20)
        date=fake.date_time_between(start_date='-2y', end_date='now')
        w.writerow([rid,pid,uid,rating,text,date.isoformat()])
        rid+=1

print("CSV generation complete.")
