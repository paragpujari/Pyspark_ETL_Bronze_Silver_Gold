import csv
import os

os.makedirs("data", exist_ok=True)

customers = [
    ["customer_id", "customer_name", "city", "state", "email"],
    ["C001", "Aarav Sharma", "Hyderabad", "Telangana", "aarav@example.com"],
    ["C002", "Diya Reddy", "Bengaluru", "Karnataka", "diya@example.com"],
    ["C003", "Kabir Das", "Bhubaneswar", "Odisha", "kabir@example.com"],
    ["C004", "Meera Nair", "Chennai", "Tamil Nadu", "meera@example.com"],
]
products = [
    ["product_id", "product_name", "category", "list_price"],
    ["P101", "Laptop", "Electronics", "65000"],
    ["P102", "Headphones", "Electronics", "2500"],
    ["P103", "Office Chair", "Furniture", "8500"],
    ["P104", "Notebook", "Stationery", "120"],
]
orders = [
    ["order_id", "customer_id", "product_id", "order_date", "quantity", "unit_price", "status"],
    ["O1001", "C001", "P101", "2026-01-05", "1", "62000", "Completed"],
    ["O1002", "C002", "P102", "2026-01-05", "2", "2300", "Completed"],
    ["O1003", "C001", "P103", "2026-01-07", "1", "8200", "Shipped"],
    ["O1004", "C003", "P104", "2026-01-08", "10", "100", "Completed"],
    ["O1005", "C004", "P102", "2026-02-02", "1", "2400", "Completed"],
    ["O1006", "C009", "P101", "2026-02-03", "1", "61000", "Completed"],  # invalid customer
    ["O1007", "C002", "P103", "2026-02-04", "-2", "8000", "Completed"],   # invalid quantity
    ["O1008", "C003", "P999", "2026-02-05", "1", "500", "Completed"],     # invalid product
    ["O1009", "C001", "P102", "not-a-date", "1", "2200", "Completed"],   # invalid date
    ["O1010", "C004", "P104", "2026-02-07", "3", "-5", "Completed"],     # invalid price
    ["O1002", "C002", "P102", "2026-01-05", "2", "2300", "Completed"],   # duplicate
]

def write_csv(name, rows):
    with open(os.path.join("data", name), "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerows(rows)

write_csv("customers.csv", customers)
write_csv("products.csv", products)
write_csv("orders.csv", orders)
print("Sample CSV files created in ./data")
