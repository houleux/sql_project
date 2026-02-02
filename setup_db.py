import sqlite3
import random
from datetime import datetime, timedelta

conn = sqlite3.connect('saas_crm.db')
cursor = conn.cursor()


# clean slate
cursor.executescript('''
    DROP TABLE IF EXISTS support_tickets;
    DROP TABLE IF EXISTS customers;
    DROP TABLE IF EXISTS subscriptions;
''')



# define schema
cursor.executescript('''
    CREATE TABLE customers (
        customer_id INTEGER PRIMARY KEY,
        name TEXT,
        industry TEXT,
        signup_date DATE
    );

    CREATE TABLE subscriptions (
        sub_id INTEGER PRIMARY KEY,
        customer_id INTEGER,
        plan_tier TEXT CHECK(plan_tier IN ('Basic', 'Pro', 'Enterprise')),
        monthly_revenue REAL,
        status TEXT CHECK(status IN ('Active', 'Churned', 'Past Due')),
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
    );

    CREATE TABLE support_tickets (
        ticket_id INTEGER PRIMARY KEY,
        customer_id INTEGER,
        issue_type TEXT,
        priority TEXT CHECK(priority IN ('Low', 'Medium', 'High')),
        resolved_in_hours INTEGER,
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
    );
''')

# 3. SEED DUMMY DATA (So we can test queries)
industries = ['Tech', 'Healthcare', 'Finance', 'Retail']
plans = [('Basic', 29.99), ('Pro', 99.99), ('Enterprise', 499.99)]
statuses = ['Active', 'Active', 'Active', 'Churned', 'Past Due']

print("Seeding database...")
for i in range(1, 101): 
    # Insert Customer
    ind = random.choice(industries)
    date = (datetime.now() - timedelta(days=random.randint(0, 700))).strftime('%Y-%m-%d')
    cursor.execute("INSERT INTO customers (name, industry, signup_date) VALUES (?, ?, ?)",
                   (f"Company_{i}", ind, date))
    
    # Insert Subscription
    plan, cost = random.choice(plans)
    status = random.choice(statuses)
    cursor.execute("INSERT INTO subscriptions (customer_id, plan_tier, monthly_revenue, status) VALUES (?, ?, ?, ?)",
                   (i, plan, cost, status))
    
    # Insert Tickets (0 to 3 tickets per customer)
    for _ in range(random.randint(0, 3)):
        prio = random.choice(['Low', 'Medium', 'High'])
        hours = random.randint(1, 48)
        cursor.execute("INSERT INTO support_tickets (customer_id, issue_type, priority, resolved_in_hours) VALUES (?, ?, ?, ?)",
                       (i, "Bug", prio, hours))

conn.commit()
print("Database 'saas_crm.db' created successfully with dummy data.")
conn.close()