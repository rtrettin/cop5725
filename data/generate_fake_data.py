# Python 3

# Simple script to generate clean and dirty data to test
# the Cleanix proof of concept implementation

from faker import Faker
import pymysql.cursors
import random

HOST = 'localhost'
USER = ''
PASSWORD = ''
DB1 = ''
DB2 = ''

print("This script will fill two MySQL databases with fake dirty data.")
numclean = input("How many clean records do you want created? ")
numdirty = input("How many dirty records do you want created? ")
numclean = int(numclean)
numdirty = int(numdirty)

connection = pymysql.connect(host=HOST,user=USER,password=PASSWORD,db=DB1,charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)
fake = Faker()

with connection.cursor() as cursor:
    cursor.execute("DROP TABLE IF EXISTS `customers`")
    cursor.execute("DROP TABLE IF EXISTS `balance`")
    cursor.execute("DROP TABLE IF EXISTS `account`")
    cursor.execute("CREATE TABLE `account` (`id` int(11) NOT NULL AUTO_INCREMENT, `customer_id` int(11) NOT NULL, `last_transaction` datetime NULL, `cc_expire` varchar(10) NOT NULL, `cc_num` varchar(32) NOT NULL, `cc_ccv` varchar(16) NOT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=latin1")
    cursor.execute("CREATE TABLE `balance` (`id` int(11) NOT NULL AUTO_INCREMENT, `customer_id` int(11) NOT NULL, `currency` float NOT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=latin1")
    cursor.execute("CREATE TABLE `customers` (`id` int(11) NOT NULL AUTO_INCREMENT, `full_name` varchar(100) NOT NULL, `address` varchar(100) NOT NULL, `phone_number` varchar(50) NOT NULL, `employer` varchar(100) NOT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=latin1")
connection.commit()

for _ in range(1, numclean+1):
    print("Inserting clean record "+str(_)+" of "+str(numclean)+"...")
    full_name = fake.name()
    address = fake.address()
    phone_number = fake.phone_number().split("x")[0]
    employer = fake.company()
    last_transaction = fake.iso8601(tzinfo=None, end_datetime=None)
    cc_expire = fake.credit_card_expire(start="now", end="+10y", date_format="%m/%y")
    cc_num = fake.credit_card_number(card_type=None)
    cc_ccv = fake.credit_card_security_code(card_type=None)
    currency = round(random.uniform(25, 100000), 2)
    with connection.cursor() as cursor:
        sql = "INSERT INTO `customers` (`full_name`, `address`, `phone_number`, `employer`) VALUES (%s, %s, %s, %s)"
        cursor.execute(sql, (full_name, address, phone_number, employer))
    connection.commit()
    with connection.cursor() as cursor:
        sql = "INSERT INTO `account` (`customer_id`, `last_transaction`, `cc_expire`, `cc_num`, `cc_ccv`) VALUES ("+str(_)+", %s, %s, %s, %s)"
        cursor.execute(sql, (last_transaction, cc_expire, cc_num, cc_ccv))
    connection.commit()
    with connection.cursor() as cursor:
        sql = "INSERT INTO `balance` (`customer_id`, `currency`) VALUES ("+str(_)+", "+str(currency)+")"
        cursor.execute(sql)
    connection.commit()

i = 1
for _ in range(numclean+1, numclean+1+numdirty):
    print("Inserting dirty record "+str(i)+" of "+str(numdirty)+"...")
    full_name = fake.name()
    address = fake.address()
    phone_number = fake.phone_number().split("x")[0]
    employer = '' # 100% chance for empty employer string
    last_transaction = '1900-01-01 10:00:00' # 100% chance to have wrong date
    cc_expire = fake.credit_card_expire(start="now", end="+10y", date_format="%m/%y")
    if random.random() <= 0.3: # 30% chance to strip year from date
        cc_expire = cc_expire.split("/")[0]
    cc_num = fake.credit_card_number(card_type=None)
    cc_ccv = fake.credit_card_security_code(card_type=None)
    if random.random() <= 0.3: # 30% chance to change ccv code
        cc_ccv = 'abc'
    currency = round(random.uniform(25, 100000), 2)
    if random.random() <= 0.5: # 50% chance to negate money balance
        currency = -currency
    with connection.cursor() as cursor:
        sql = "INSERT INTO `customers` (`full_name`, `address`, `phone_number`, `employer`) VALUES (%s, %s, %s, %s)"
        cursor.execute(sql, (full_name, address, phone_number, employer))
    connection.commit()
    with connection.cursor() as cursor:
        sql = "INSERT INTO `account` (`customer_id`, `last_transaction`, `cc_expire`, `cc_num`, `cc_ccv`) VALUES ("+str(_)+", %s, %s, %s, %s)"
        cursor.execute(sql, (last_transaction, cc_expire, cc_num, cc_ccv))
    connection.commit()
    with connection.cursor() as cursor:
        sql = "INSERT INTO `balance` (`customer_id`, `currency`) VALUES ("+str(_)+", "+str(currency)+")"
        cursor.execute(sql)
    connection.commit()
    if random.random() <= 0.3: # 30% chance to insert exact duplicates
        with connection.cursor() as cursor:
            sql = "INSERT INTO `customers` (`full_name`, `address`, `phone_number`, `employer`) VALUES (%s, %s, %s, %s)"
            cursor.execute(sql, (full_name, address, phone_number, employer))
        connection.commit()
        with connection.cursor() as cursor:
            sql = "INSERT INTO `account` (`customer_id`, `last_transaction`, `cc_expire`, `cc_num`, `cc_ccv`) VALUES ("+str(_)+", %s, %s, %s, %s)"
            cursor.execute(sql, (last_transaction, cc_expire, cc_num, cc_ccv))
        connection.commit()
        with connection.cursor() as cursor:
            sql = "INSERT INTO `balance` (`customer_id`, `currency`) VALUES ("+str(_)+", "+str(currency)+")"
            cursor.execute(sql)
        connection.commit()
    i += 1

connection.close()

connection = pymysql.connect(host=HOST,user=USER,password=PASSWORD,db=DB2,charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)

with connection.cursor() as cursor:
    cursor.execute("DROP TABLE IF EXISTS `customers`")
    cursor.execute("CREATE TABLE `customers` (`id` int(11) NOT NULL AUTO_INCREMENT, `full_name` varchar(100) NOT NULL, `address` varchar(100) NOT NULL, `phone_number` varchar(50) NOT NULL, `employer` varchar(100) NOT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=latin1")
connection.commit()

for _ in range(1, numclean+1):
    print("Inserting clean (duplicate key) record "+str(_)+" of "+str(numclean)+"...")
    full_name = fake.name()
    address = fake.address()
    phone_number = fake.phone_number().split("x")[0]
    employer = fake.company()
    with connection.cursor() as cursor:
        sql = "INSERT INTO `customers` (`full_name`, `address`, `phone_number`, `employer`) VALUES (%s, %s, %s, %s)"
        cursor.execute(sql, (full_name, address, phone_number, employer))
    connection.commit()

i = 1
for _ in range(numclean+1, numclean+1+numdirty):
    print("Inserting dirty (duplicate key) record "+str(i)+" of "+str(numdirty)+"...")
    full_name = fake.name()
    address = fake.address()
    phone_number = fake.phone_number()
    employer = fake.company()
    with connection.cursor() as cursor:
        sql = "INSERT INTO `customers` (`full_name`, `address`, `phone_number`, `employer`) VALUES (%s, %s, %s, %s)"
        cursor.execute(sql, (full_name, address, phone_number, employer))
    connection.commit()
    i += 1

connection.close()