# Copyright © 2026 Pathway

import asyncio
import json
import random
import time

RABBITMQ_HOST = "rabbitmq"
RABBITMQ_PORT = 5552
RABBITMQ_USER = "guest"
RABBITMQ_PASSWORD = "guest"

STREAMS = ["employees_raw", "orders_raw", "listings_raw"]

# Sample employees - some with intentional data quality issues for validation demo
EMPLOYEES = [
    {
        "employee_id": 1,
        "first_name": "Alice",
        "last_name": "Smith",
        "email": "ALICE.SMITH@COMPANY.COM",
        "phone": "+1 (555) 123-4567",
        "department": "Engineering",
        "salary": "95000",
        "hire_date": "2022-01-15",
        "status": "active",
    },
    {
        "employee_id": 2,
        "first_name": "Bob",
        "last_name": "Jones",
        "email": "bob.jones@company.com",
        "phone": "555-234-5678",
        "department": "Sales",
        "salary": "72000",
        "hire_date": "2021-06-20",
        "status": "active",
    },
    {
        # Bad record: empty first_name, invalid email, negative salary
        "employee_id": 3,
        "first_name": "",
        "last_name": "Garcia",
        "email": "invalid-email",
        "phone": "555-345-6789",
        "department": "Marketing",
        "salary": "-5000",
        "hire_date": "2023-03-01",
        "status": "active",
    },
    {
        "employee_id": 4,
        "first_name": "Diana",
        "last_name": "Lee",
        "email": "diana.lee@company.com",
        "phone": "+44 20 7946 0958",
        "department": "ENGINEERING",
        "salary": "105000",
        "hire_date": "2020-11-10",
        "status": "active",
    },
    {
        "employee_id": 5,
        "first_name": "Erik",
        "last_name": "Martinez",
        "email": "erik.martinez@company.com",
        "phone": "555-456-7890",
        "department": "Sales",
        "salary": "68000",
        "hire_date": "2023-08-05",
        "status": "terminated",
    },
    {
        # Bad record: invalid phone
        "employee_id": 6,
        "first_name": "Fatima",
        "last_name": "Ahmed",
        "email": "fatima.ahmed@company.com",
        "phone": "abc",
        "department": "engineering",
        "salary": "88000",
        "hire_date": "2022-04-18",
        "status": "active",
    },
    {
        "employee_id": 7,
        "first_name": "George",
        "last_name": "Wilson",
        "email": "george.wilson@company.com",
        "phone": "+1-555-567-8901",
        "department": "Real Estate",
        "salary": "78000",
        "hire_date": "2021-09-30",
        "status": "active",
    },
    {
        "employee_id": 8,
        "first_name": "Hannah",
        "last_name": "Brown",
        "email": "hannah.brown@company.com",
        "phone": "555-678-9012",
        "department": "Real Estate",
        "salary": "82000",
        "hire_date": "2022-07-12",
        "status": "active",
    },
    {
        # Bad record: empty salary
        "employee_id": 9,
        "first_name": "Ivan",
        "last_name": "Petrov",
        "email": "ivan.petrov@company.com",
        "phone": "+7 495 123 4567",
        "department": "Marketing",
        "salary": "",
        "hire_date": "2023-01-22",
        "status": "active",
    },
    {
        "employee_id": 10,
        "first_name": "Julia",
        "last_name": "Chen",
        "email": "julia.chen@company.com",
        "phone": "+86 10 1234 5678",
        "department": "Engineering",
        "salary": "115000",
        "hire_date": "2019-05-14",
        "status": "active",
    },
]

# Sample orders - multiple per employee
ORDERS = [
    {"order_id": 101, "employee_id": 1, "amount": "1250.50", "order_date": "2024-11-01", "product": "Laptop Stand", "quantity": 2},
    {"order_id": 102, "employee_id": 2, "amount": "340.00", "order_date": "2024-11-05", "product": "Keyboard", "quantity": 5},
    {"order_id": 103, "employee_id": 1, "amount": "89.99", "order_date": "2024-11-08", "product": "Mouse Pad", "quantity": 10},
    {"order_id": 104, "employee_id": 4, "amount": "2100.00", "order_date": "2024-11-10", "product": "Monitor", "quantity": 1},
    {"order_id": 105, "employee_id": 5, "amount": "450.00", "order_date": "2024-11-12", "product": "Headset", "quantity": 3},
    {"order_id": 106, "employee_id": 2, "amount": "175.25", "order_date": "2024-11-15", "product": "Webcam", "quantity": 2},
    {"order_id": 107, "employee_id": 7, "amount": "560.00", "order_date": "2024-11-18", "product": "Desk Lamp", "quantity": 4},
    {"order_id": 108, "employee_id": 10, "amount": "3200.00", "order_date": "2024-11-20", "product": "Standing Desk", "quantity": 1},
    {"order_id": 109, "employee_id": 4, "amount": "125.50", "order_date": "2024-11-22", "product": "Cable Kit", "quantity": 8},
    {"order_id": 110, "employee_id": 1, "amount": "780.00", "order_date": "2024-11-25", "product": "Chair Mat", "quantity": 2},
    {"order_id": 111, "employee_id": 8, "amount": "95.00", "order_date": "2024-11-28", "product": "Notebook", "quantity": 15},
    {"order_id": 112, "employee_id": 10, "amount": "420.00", "order_date": "2024-12-01", "product": "USB Hub", "quantity": 6},
    {"order_id": 113, "employee_id": 7, "amount": "1800.00", "order_date": "2024-12-03", "product": "Printer", "quantity": 1},
    {"order_id": 114, "employee_id": 2, "amount": "65.99", "order_date": "2024-12-05", "product": "Mouse", "quantity": 3},
    {"order_id": 115, "employee_id": 4, "amount": "290.00", "order_date": "2024-12-08", "product": "Speakers", "quantity": 2},
]

# Sample rental listings - inspired by ldtrungmark rental platform
LISTINGS = [
    {"listing_id": 1001, "title": "Modern 2BR Apartment Downtown", "price": "2500.00", "location": "District 1", "source": "website_a", "posted_date": "2024-11-01", "agent_employee_id": 7},
    {"listing_id": 1002, "title": "Cozy Studio Near Park", "price": "1200.00", "location": "District 3", "source": "website_b", "posted_date": "2024-11-03", "agent_employee_id": 8},
    {"listing_id": 1003, "title": "Spacious 3BR Family Home", "price": "3800.00", "location": "District 2", "source": "website_a", "posted_date": "2024-11-05", "agent_employee_id": 7},
    # Bad record: negative price
    {"listing_id": 1004, "title": "Invalid Listing", "price": "-500.00", "location": "District 5", "source": "website_c", "posted_date": "2024-11-07", "agent_employee_id": 8},
    {"listing_id": 1005, "title": "Luxury Penthouse with View", "price": "8500.00", "location": "District 1", "source": "website_b", "posted_date": "2024-11-10", "agent_employee_id": 7},
    {"listing_id": 1006, "title": "Renovated 1BR Near Metro", "price": "1800.00", "location": "District 4", "source": "website_a", "posted_date": "2024-11-12", "agent_employee_id": 8},
    {"listing_id": 1007, "title": "Garden Villa with Pool", "price": "12000.00", "location": "District 7", "source": "website_b", "posted_date": "2024-11-15", "agent_employee_id": 7},
    {"listing_id": 1008, "title": "Budget Room in Shared House", "price": "450.00", "location": "District 9", "source": "website_a", "posted_date": "2024-11-18", "agent_employee_id": 8},
    # Bad record: empty title
    {"listing_id": 1009, "title": "", "price": "1500.00", "location": "District 3", "source": "website_c", "posted_date": "2024-11-20", "agent_employee_id": 7},
    {"listing_id": 1010, "title": "Furnished Office Space", "price": "5000.00", "location": "District 1", "source": "website_a", "posted_date": "2024-11-22", "agent_employee_id": 8},
]

PRODUCTS = ["Laptop Stand", "Keyboard", "Mouse", "Monitor", "Headset", "Webcam", "USB Hub", "Chair Mat"]
LOCATIONS = ["District 1", "District 2", "District 3", "District 4", "District 7", "District 9"]
SOURCES = ["website_a", "website_b"]


async def main():
    from rstream import AMQPMessage, Producer

    print("Connecting to RabbitMQ Streams...")
    producer = Producer(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        username=RABBITMQ_USER,
        password=RABBITMQ_PASSWORD,
    )
    await producer.start()

    # Create all streams
    for stream in STREAMS:
        try:
            await producer.create_stream(stream)
            print(f"Created stream: {stream}")
        except Exception:
            print(f"Stream already exists: {stream}")

    # Publish initial employees
    print("Publishing employee records...")
    for emp in EMPLOYEES:
        await producer.send("employees_raw", AMQPMessage(body=json.dumps(emp).encode()))
        await asyncio.sleep(0.3)
    print(f"Published {len(EMPLOYEES)} employees")

    # Publish initial orders
    print("Publishing order records...")
    for order in ORDERS:
        await producer.send("orders_raw", AMQPMessage(body=json.dumps(order).encode()))
        await asyncio.sleep(0.2)
    print(f"Published {len(ORDERS)} orders")

    # Publish initial listings
    print("Publishing listing records...")
    for listing in LISTINGS:
        await producer.send("listings_raw", AMQPMessage(body=json.dumps(listing).encode()))
        await asyncio.sleep(0.2)
    print(f"Published {len(LISTINGS)} listings")

    # Continue producing new data periodically to demonstrate streaming
    print("Starting continuous data production...")
    order_id = 200
    listing_id = 2000
    valid_employee_ids = [1, 2, 4, 5, 7, 8, 10]
    agent_ids = [7, 8]

    while True:
        await asyncio.sleep(5)

        # New order
        order_id += 1
        new_order = {
            "order_id": order_id,
            "employee_id": random.choice(valid_employee_ids),
            "amount": f"{random.uniform(50, 2000):.2f}",
            "order_date": "2024-12-10",
            "product": random.choice(PRODUCTS),
            "quantity": random.randint(1, 10),
        }
        await producer.send("orders_raw", AMQPMessage(body=json.dumps(new_order).encode()))
        print(f"Published new order: {order_id}")

        await asyncio.sleep(3)

        # New listing
        listing_id += 1
        new_listing = {
            "listing_id": listing_id,
            "title": f"New Listing #{listing_id}",
            "price": f"{random.uniform(500, 10000):.2f}",
            "location": random.choice(LOCATIONS),
            "source": random.choice(SOURCES),
            "posted_date": "2024-12-10",
            "agent_employee_id": random.choice(agent_ids),
        }
        await producer.send("listings_raw", AMQPMessage(body=json.dumps(new_listing).encode()))
        print(f"Published new listing: {listing_id}")


if __name__ == "__main__":
    time.sleep(5)  # Extra wait for RabbitMQ streams plugin to initialize
    asyncio.run(main())
