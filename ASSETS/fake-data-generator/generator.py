import random
import datetime
import pandas as pd
from faker import Faker

FAKE = Faker()

class FakeRow:
    def __init__(self, products_dict, customers_dict, branches_dict, row_n):
        self.products_dict = products_dict
        self.customers_dict = customers_dict
        self.branches_dict = branches_dict
        self.row_n = row_n
        self.df = self.generate_fake_csv()

    def generate_fake_row(self) -> dict:
        product_info = random.choice(self.products_dict)
        customer_info = random.choice(self.customers_dict)
        branch_info = random.choice(self.branches_dict)

        invoice_id = int(random.randint(100, 6000))
        branch = branch_info["branch"]
        city = branch_info["city"]
        salesman = random.choice(branch_info["salesmen"])
        salesman_fname = salesman.split(' ')[0]
        salesman_lname = salesman.split(' ')[1]
        first_name = customer_info["firstname"]
        last_name = customer_info["lastname"]
        email = customer_info["email"]
        phone_number = customer_info["phone"]
        product_line = product_info["line"]
        product_name = product_info["product"]
        unit_price = product_info["price"]
        quantity = random.randint(1, 10)
        date = datetime.datetime.today().strftime('%Y-%m-%d')
        time = FAKE.time(pattern='%H:%M:%S')
        payment_method = random.choice(['Cash', 'Credit Card', 'E-wallet'])

        return {
            "Invoice_ID": invoice_id,
            "Branch": branch,
            "City": city,
            "First_name": first_name,
            "Last_name": last_name,
            "Salesman_firstname": salesman_fname,
            "Salesman_lastname": salesman_lname,
            "Email": email,
            "Phone_number": phone_number,
            "product": product_name,
            "Product_line": product_line,
            "Unit_price": unit_price,
            "Quantity": quantity,
            "Date": date,
            "Time": time,
            "Payment": payment_method,
        }

    def generate_fake_csv(self) -> pd.DataFrame:
        columns = self.generate_fake_row().keys()
        data_dict = {column: [] for column in columns}

        for _ in range(self.row_n):
            fake_row_dict = self.generate_fake_row()
            for key, value in fake_row_dict.items():
                data_dict[key].append(value)

        return pd.DataFrame(data_dict)

    # Increase probability of null values
    def add_random_nulls(self, probability=0.01):
        for col in self.df.columns:
            self.df.loc[self.df.sample(frac=probability).index, col] = None

    # The commented section needs some fixes
    # # Introduce incorrect data types
    # def ruin_datatypes(self, probability=0.002, cols=["Invoice_ID", "Unit_price", "Quantity"]):
    #     for col in cols:
    #         self.df.loc[self.df.sample(frac=probability).index, col] = self.df[col].apply(lambda x: FAKE.word())

    # # Introduce outliers
    # def generate_outliers(self, probability=0.002, cols=["Unit_price", "Quantity"]):
    #     for col in cols:
    #         self.df.loc[self.df.sample(frac=probability).index, col] = self.df[col].apply(lambda x: x * random.randint(10, 100))

    # # Add duplicate rows
    # def add_duplicates(self, probability=0.002):
    #     duplicates = self.df.sample(frac=probability)
    #     self.df = pd.concat([self.df, duplicates], ignore_index=True)

    # # Inconsistent date formatting
    # def ruin_formatting(self, probability=0.002):
    #     date_formats = ['%d-%m-%Y', '%m/%d/%Y', '%Y/%m/%d']
    #     self.df.loc[self.df.sample(frac=probability).index, 'Date'] = self.df['Date'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d').strftime(random.choice(date_formats)))

    # # Typographical errors
    # def add_typos(self, probability=0.002):
    #     def introduce_typo(s):
    #         if not s or len(s) < 3:
    #             return s
    #         pos = random.randint(0, len(s) - 2)
    #         return s[:pos] + s[pos + 1] + s[pos] + s[pos + 2:]

    #     for col in ["First_name", "Last_name", "City", "Branch"]:
    #         self.df.loc[self.df.sample(frac=probability).index, col] = self.df[col].apply(introduce_typo)

    # # Invalid values
    # def add_invalid_values(self, probability=0.002):
    #     self.df.loc[self.df.sample(frac=probability).index, "Quantity"] = self.df["Quantity"].apply(lambda x: -random.randint(1, 10))


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

row_n = 2000

Fake = FakeRow(_products_dict,
                _customers_dict, 
                _branches_dict, 
                row_n)

Fake.add_random_nulls()
# Fake.add_duplicates()
# Fake.add_invalid_values()
# Fake.add_typos()
# Fake.ruin_datatypes()
# Fake.ruin_formatting()
Fake.df.to_csv(f'./STAGES/LANDED/{datetime.datetime.now()}.csv', index=False)