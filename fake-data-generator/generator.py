import random
import datetime
import pandas as pd
from faker import Faker


row_n = 2000
FAKE = Faker()

_products_dict = [
    {"product": "Laptop", "line": "Electronics", "price": 1000},
    {"product": "Smartphone", "line": "Electronics", "price": 800},
    {"product": "Shirt", "line": "Clothing", "price": 30},
    {"product": "Dress", "line": "Clothing", "price": 50},
    {"product": "Camera", "line": "Electronics", "price": 600},
    {"product": "Trousers", "line": "Clothing", "price": 40},
    {"product": "Fiction Book", "line": "Books", "price": 20},
    {"product": "Non-Fiction Book", "line": "Books", "price": 25},
    {"product": "Football", "line": "Sports", "price": 15},
    {"product": "Perfume", "line": "Beauty", "price": 50},
]

_customers_dict = [
    {"firstname": "John", "lastname": "Doe", "email": "john.doe@example.com", "phone": "+1234567890", "rating": 7},
    {"firstname": "Jane", "lastname": "Smith", "email": "jane.smith@example.com", "phone": "+1987654321", "rating": 9},
    {"firstname": "Michael", "lastname": "Johnson", "email": "michael.johnson@example.com", "phone": "+1122334455", "rating": 8},
    {"firstname": "Emily", "lastname": "Brown", "email": "emily.brown@example.com", "phone": "+9988776655", "rating": 6},
    {"firstname": "David", "lastname": "Jones", "email": "david.jones@example.com", "phone": "+5544332211", "rating": 9},
    {"firstname": "Sarah", "lastname": "Miller", "email": "sarah.miller@example.com", "phone": "+6677889900", "rating": 7},
    {"firstname": "Christopher", "lastname": "Davis", "email": "christopher.davis@example.com", "phone": "+1122334455", "rating": 8},
    {"firstname": "Amanda", "lastname": "Wilson", "email": "amanda.wilson@example.com", "phone": "+9988776655", "rating": 6},
    {"firstname": "Matthew", "lastname": "Taylor", "email": "matthew.taylor@example.com", "phone": "+5544332211", "rating": 9},
    {"firstname": "Lauren", "lastname": "Anderson", "email": "lauren.anderson@example.com", "phone": "+6677889900", "rating": 7},
]

_branches_dict = [
    {"branch": "Branch A", "city": "New York", "salesmen": ["John Doe", "Jane Smith", "Alex Johnson"]},
    {"branch": "Branch B", "city": "Los Angeles", "salesmen": ["Emily Davis", "Michael Brown", "Jessica Wilson"]},
    {"branch": "Branch C", "city": "Chicago", "salesmen": ["Sarah Martinez", "David Lee", "Daniel Garcia"]},
    {"branch": "Branch D", "city": "Houston", "salesmen": ["Matthew Thomas", "Laura Martinez", "Andrew Hernandez"]},
    {"branch": "Branch E", "city": "Phoenix", "salesmen": ["Olivia Lopez", "James Clark", "Sophia Lewis"]},
    {"branch": "Branch F", "city": "Philadelphia", "salesmen": ["Mia Walker", "Benjamin Hall", "Lucas Allen"]},
    {"branch": "Branch G", "city": "San Antonio", "salesmen": ["Amelia Young", "Ethan King", "Ella Wright"]},
    {"branch": "Branch H", "city": "San Diego", "salesmen": ["Madison Scott", "Jack Robinson", "Chloe Perez"]},
    {"branch": "Branch I", "city": "Dallas", "salesmen": ["Avery Harris", "Logan Green", "Grace Adams"]},
    {"branch": "Branch J", "city": "San Jose", "salesmen": ["Liam Nelson", "Isabella Hill", "Henry Baker"]},
]


class Fact_Fake_Row:
    def __init__(self, products_dict, customers_dict, branches_dict):
        self.product_info = random.choice(products_dict)
        self.customer_info = random.choice(customers_dict)
        self.branch_info = random.choice(branches_dict)
        self.invoice_id = int(random.randint(100, 6000))
        self.branch = self.branch_info["branch"]
        self.city = self.branch_info["city"]
        self.salesman = random.choice(self.branch_info["salesmen"])
        self.salesman_fname = self.salesman.split(' ')[0]
        self.salesman_lname = self.salesman.split(' ')[1]
        # self.customer_type = random.choice(['Member', 'Normal'])
        self.first_name = self.customer_info["firstname"]
        self.last_name = self.customer_info["lastname"]
        self.email = self.customer_info["email"]
        self.phone_number = self.customer_info["phone"]
        self.rating = self.customer_info["rating"]
        self.gender = random.choice(['Male', 'Female'])
        self.product_line = self.product_info["line"]
        self.product_name = self.product_info["product"]
        self.unit_price = self.product_info["price"]
        self.quantity = random.randint(1, 10)
        self.tax = round(self.unit_price * self.quantity * 0.05, 2)
        self.total = round(self.unit_price * self.quantity + self.tax, 2)
        self.date = FAKE.date_this_year()
        self.time = FAKE.time(pattern='%H:%M:%S')
        self.payment_method = random.choice(['Cash', 'Credit Card', 'E-wallet'])
        self.cogs = round(self.total / 1.05, 2)
        self.gross_margin_percentage = round(random.uniform(5, 40), 2)
        self.gross_income = round(self.total - self.cogs, 2)
    
    def get_FAKE_row(self):
        return {
        "Invoice_ID": self.invoice_id,
        "Branch":self. branch,
        "City": self.city,
        "First_name": self.first_name,
        "Last_name": self.last_name,
        "Salesman_firstname": self.salesman_fname,
        "Salesman_lastname": self.salesman_lname,
        "Email": self.email,
        "Phone_number": self.phone_number,
        # "Gender":self. gender,
        "product": self.product_name,
        "Product_line":self. product_line,
        "Unit_price": self.unit_price,
        "Quantity": self.quantity,
        # "Tax_5%": self.tax,
        # "Total": self.total,
        "Date": self.date,
        "Time": self.time,
        "Payment": self.payment_method,
        # "cogs": self.cogs,
        # "gross_margin_percentage":self. gross_margin_percentage,
        # "gross_income": self.gross_income,
        # "Rating": self.rating
        }


generator = Fact_Fake_Row(_products_dict, _customers_dict, _branches_dict)
columns = generator.get_FAKE_row().keys()

data_dict = {column: [] for column in columns}

for _ in range(row_n):
    fake_row_dict = Fact_Fake_Row(_products_dict, _customers_dict, _branches_dict).get_FAKE_row()
    for key in fake_row_dict.keys():
        data_dict[key].append(fake_row_dict[key])

df = pd.DataFrame(data_dict)

null_prob = {key: random.uniform(0, 0.002) for key in data_dict.keys()}
for col, prob in null_prob.items():
    df[col] = df[col].apply(lambda x: x if ((random.uniform(0, 1) > prob)) else None)

df.to_csv(f'{datetime.datetime.now()}.csv', index=False)