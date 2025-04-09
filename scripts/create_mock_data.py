# -- Create schema (if not exists needs dynamic SQL workaround; skipped here for simplicity)
# IF NOT EXISTS (
#     SELECT * FROM sys.schemas WHERE name = 'EXL_SCHEMA'
# )
# BEGIN
#     EXEC('CREATE SCHEMA EXL_SCHEMA');
# END
# GO

# -- Use the database
# USE EXL_DB;
# GO

# -- Create tables under the schema
# IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Customers' AND SCHEMA_NAME(schema_id) = 'EXL_SCHEMA')
# BEGIN
#     CREATE TABLE EXL_SCHEMA.Customers (
#         CustomerID INT PRIMARY KEY,
#         Name NVARCHAR(100) NOT NULL,
#         Email NVARCHAR(100),
#         Phone NVARCHAR(20)
#     );
# END
# GO

# IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Accounts' AND SCHEMA_NAME(schema_id) = 'EXL_SCHEMA')
# BEGIN
#     CREATE TABLE EXL_SCHEMA.Accounts (
#         AccountID INT PRIMARY KEY,
#         CustomerID INT,
#         AccountType NVARCHAR(50),
#         Balance FLOAT,
#         FOREIGN KEY (CustomerID) REFERENCES EXL_SCHEMA.Customers(CustomerID)
#     );
# END
# GO

# IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Transactions' AND SCHEMA_NAME(schema_id) = 'EXL_SCHEMA')
# BEGIN
#     CREATE TABLE EXL_SCHEMA.Transactions (
#         TransactionID INT PRIMARY KEY,
#         AccountID INT,
#         TransactionDate DATE,
#         Amount FLOAT,
#         Type NVARCHAR(20),
#         FOREIGN KEY (AccountID) REFERENCES EXL_SCHEMA.Accounts(AccountID)
#     );
# END
# GO

# IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Loans' AND SCHEMA_NAME(schema_id) = 'EXL_SCHEMA')
# BEGIN
#     CREATE TABLE EXL_SCHEMA.Loans (
#         LoanID INT PRIMARY KEY,
#         CustomerID INT,
#         LoanAmount FLOAT,
#         InterestRate FLOAT,
#         StartDate DATE,
#         EndDate DATE,
#         FOREIGN KEY (CustomerID) REFERENCES EXL_SCHEMA.Customers(CustomerID)
#     );
# END
# GO


# INSERT INTO EXL_SCHEMA.Customers (CustomerID, Name, Email, Phone) VALUES
#         (1, 'Alice Johnson', 'alice@example.com', '123-456-7890'),
#         (2, 'Bob Smith', 'bob@example.com', '234-567-8901'),
#         (3, 'Charlie Brown', 'charlie@example.com', '345-678-9012'),
#         (4, 'David Lee', 'david@example.com', '456-789-0123'),
#         (5, 'Eva Green', 'eva@example.com', '567-890-1234'),
#         (6, 'Frank Wright', 'frank@example.com', '678-901-2345'),
#         (7, 'Grace Kim', 'grace@example.com', '789-012-3456'),
#         (8, 'Helen Park', 'helen@example.com', '890-123-4567'),
#         (9, 'Ian Black', 'ian@example.com', '901-234-5678'),
#         (10, 'Judy White', 'judy@example.com', '012-345-6789'),
#         (11, 'Kevin Green', 'kevin@example.com', '123-000-1111'),
#         (12, 'Linda Hall', 'linda@example.com', '234-111-2222'),
#         (13, 'Mike King', 'mike@example.com', '345-222-3333'),
#         (14, 'Nina Ford', 'nina@example.com', '456-333-4444'),
#         (15, 'Oscar Bell', 'oscar@example.com', '567-444-5555'),
#         (16, 'Paul Adams', 'paul@example.com', '678-555-6666'),
#         (17, 'Queen Carter', 'queen@example.com', '789-666-7777'),
#         (18, 'Ron Dean', 'ron@example.com', '890-777-8888'),
#         (19, 'Sara Evans', 'sara@example.com', '901-888-9999'),
#         (20, 'Tom Fox', 'tom@example.com', '012-999-0000'),
#         (21, 'Uma Nolan', 'uma@example.com', '111-222-3333'),
#         (22, 'Victor Reed', 'victor@example.com', '222-333-4444'),
#         (23, 'Wendy Cruz', 'wendy@example.com', '333-444-5555'),
#         (24, 'Xander Cole', 'xander@example.com', '444-555-6666'),
#         (25, 'Yara Singh', 'yara@example.com', '555-666-7777'),
#         (26, 'Zane Blake', 'zane@example.com', '666-777-8888'),
#         (27, 'Amy Torres', 'amy@example.com', '777-888-9999'),
#         (28, 'Ben Watts', 'ben@example.com', '888-999-0000'),
#         (29, 'Cara Price', 'cara@example.com', '999-000-1111'),
#         (30, 'Drew Neal', 'drew@example.com', '000-111-2222'),
#         (31, 'Ella Rose', 'ella@example.com', '111-000-1234'),
#         (32, 'Finn Lowe', 'finn@example.com', '222-111-2345'),
#         (33, 'Gina Webb', 'gina@example.com', '333-222-3456'),
#         (34, 'Hugo Mann', 'hugo@example.com', '444-333-4567'),
#         (35, 'Isla Cross', 'isla@example.com', '555-444-5678'),
#         (36, 'Jake Clay', 'jake@example.com', '666-555-6789'),
#         (37, 'Kira Boyd', 'kira@example.com', '777-666-7890'),
#         (38, 'Leo Nash', 'leo@example.com', '888-777-8901'),
#         (39, 'Mona Dale', 'mona@example.com', '999-888-9012'),
#         (40, 'Nate Orr', 'nate@example.com', '000-999-0123');

# INSERT INTO EXL_SCHEMA.Accounts (AccountID, CustomerID, AccountType, Balance) VALUES
#         (101, 1, 'Checking', 2500.75),
#         (102, 2, 'Savings', 5400.00),
#         (103, 3, 'Checking', 1500.50),
#         (104, 4, 'Savings', 6200.00),
#         (105, 5, 'Checking', 3000.00),
#         (106, 6, 'Savings', 4100.25),
#         (107, 7, 'Checking', 2700.00),
#         (108, 8, 'Savings', 8000.00),
#         (109, 9, 'Checking', 1900.50),
#         (110, 10, 'Savings', 3600.75),
#         (111, 11, 'Checking', 2200.00),
#         (112, 12, 'Savings', 3100.45),
#         (113, 13, 'Checking', 4700.90),
#         (114, 14, 'Savings', 5100.60),
#         (115, 15, 'Checking', 2600.40),
#         (116, 16, 'Savings', 3900.20),
#         (117, 17, 'Checking', 3300.00),
#         (118, 18, 'Savings', 5800.75),
#         (119, 19, 'Checking', 4400.60),
#         (120, 20, 'Savings', 4900.10),
#         (121, 21, 'Checking', 3100.50),
#         (122, 22, 'Savings', 5200.00),
#         (123, 23, 'Checking', 2800.25),
#         (124, 24, 'Savings', 6000.00),
#         (125, 25, 'Checking', 2950.10),
#         (126, 26, 'Savings', 4300.90),
#         (127, 27, 'Checking', 3500.75),
#         (128, 28, 'Savings', 4700.60),
#         (129, 29, 'Checking', 3900.45),
#         (130, 30, 'Savings', 5100.30),
#         (131, 31, 'Checking', 2550.00),
#         (132, 32, 'Savings', 2850.85),
#         (133, 33, 'Checking', 3650.00),
#         (134, 34, 'Savings', 3250.20),
#         (135, 35, 'Checking', 4750.65),
#         (136, 36, 'Savings', 2950.40),
#         (137, 37, 'Checking', 3850.70),
#         (138, 38, 'Savings', 4450.55),
#         (139, 39, 'Checking', 3350.95),
#         (140, 40, 'Savings', 3900.00);

# INSERT INTO EXL_SCHEMA.Transactions (TransactionID, AccountID, TransactionDate, Amount, Type) VALUES 
#         (1001, 101, '2024-01-10', -200.00, 'Debit'),
#         (1002, 101, '2024-01-12', 500.00, 'Credit'),
#         (1003, 102, '2024-02-15', -150.00, 'Debit'),
#         (1004, 103, '2024-03-05', -50.00, 'Debit'),
#         (1005, 104, '2024-01-20', 600.00, 'Credit'),
#         (1006, 105, '2024-02-10', -300.00, 'Debit'),
#         (1007, 106, '2024-02-25', 800.00, 'Credit'),
#         (1008, 107, '2024-03-01', -250.00, 'Debit'),
#         (1009, 108, '2024-03-15', 900.00, 'Credit'),
#         (1010, 109, '2024-03-18', -100.00, 'Debit'),
#         (1011, 110, '2024-03-20', 400.00, 'Credit'),
#         (1012, 101, '2024-03-22', -60.00, 'Debit'),
#         (1013, 111, '2024-03-24', -30.00, 'Debit'),
#         (1014, 112, '2024-03-25', 700.00, 'Credit'),
#         (1015, 113, '2024-03-26', -120.00, 'Debit'),
#         (1016, 114, '2024-03-27', 600.00, 'Credit'),
#         (1017, 115, '2024-03-28', -80.00, 'Debit'),
#         (1018, 116, '2024-03-29', 500.00, 'Credit'),
#         (1019, 117, '2024-03-30', -90.00, 'Debit'),
#         (1020, 118, '2024-03-31', 1000.00, 'Credit'),
#         (1021, 119, '2024-04-01', -200.00, 'Debit'),
#         (1022, 120, '2024-04-02', 600.00, 'Credit'),
#         (1023, 110, '2024-04-03', -150.00, 'Debit'),
#         (1024, 101, '2024-04-04', 450.00, 'Credit'),
#         (1025, 102, '2024-04-05', -100.00, 'Debit'),
#         (1026, 121, '2024-04-06', -100.00, 'Debit'),
#         (1027, 122, '2024-04-06', 350.00, 'Credit'),
#         (1028, 123, '2024-04-07', -200.00, 'Debit'),
#         (1029, 124, '2024-04-07', 400.00, 'Credit'),
#         (1030, 125, '2024-04-08', -50.00, 'Debit'),
#         (1031, 126, '2024-04-08', 600.00, 'Credit'),
#         (1032, 127, '2024-04-09', -300.00, 'Debit'),
#         (1033, 128, '2024-04-09', 750.00, 'Credit'),
#         (1034, 129, '2024-04-10', -120.00, 'Debit'),
#         (1035, 130, '2024-04-10', 680.00, 'Credit'),
#         (1036, 131, '2024-04-11', -90.00, 'Debit'),
#         (1037, 132, '2024-04-11', 510.00, 'Credit'),
#         (1038, 133, '2024-04-12', -75.00, 'Debit'),
#         (1039, 134, '2024-04-12', 830.00, 'Credit'),
#         (1040, 135, '2024-04-13', -60.00, 'Debit'),
#         (1041, 136, '2024-04-13', 920.00, 'Credit'),
#         (1042, 137, '2024-04-14', -110.00, 'Debit'),
#         (1043, 138, '2024-04-14', 450.00, 'Credit'),
#         (1044, 139, '2024-04-15', -85.00, 'Debit'),
#         (1045, 140, '2024-04-15', 710.00, 'Credit'),
#         (1046, 101, '2024-04-16', -130.00, 'Debit'),
#         (1047, 102, '2024-04-16', 560.00, 'Credit'),
#         (1048, 103, '2024-04-17', -170.00, 'Debit'),
#         (1049, 104, '2024-04-17', 820.00, 'Credit'),
#         (1050, 105, '2024-04-18', -60.00, 'Debit');

# INSERT INTO EXL_SCHEMA.Loans (LoanID, CustomerID, LoanAmount, InterestRate, StartDate, EndDate) VALUES  
#         (2001, 1, 10000.00, 5.5, '2023-06-01', '2028-06-01'),
#         (2002, 2, 15000.00, 6.0, '2023-07-15', '2028-07-15'),
#         (2003, 4, 8000.00, 4.2, '2023-08-01', '2027-08-01'),
#         (2004, 5, 12000.00, 5.0, '2023-09-10', '2028-09-10'),
#         (2005, 6, 18000.00, 6.3, '2023-10-01', '2029-10-01'),
#         (2006, 8, 7500.00, 5.1, '2023-11-05', '2028-11-05'),
#         (2007, 12, 11000.00, 4.9, '2024-01-01', '2029-01-01'),
#         (2008, 14, 13500.00, 6.5, '2024-02-01', '2029-02-01'),
#         (2009, 17, 9500.00, 5.3, '2024-03-01', '2029-03-01'),
#         (2010, 19, 16000.00, 6.1, '2024-04-01', '2029-04-01'),
#         (2011, 21, 10500.00, 5.6, '2024-05-01', '2029-05-01'),
#         (2012, 22, 12000.00, 6.1, '2024-05-02', '2029-05-02'),
#         (2013, 23, 9500.00, 5.3, '2024-05-03', '2029-05-03'),
#         (2014, 24, 14200.00, 6.7, '2024-05-04', '2029-05-04'),
#         (2015, 25, 13250.00, 5.8, '2024-05-05', '2029-05-05'),
#         (2016, 26, 11700.00, 6.0, '2024-05-06', '2029-05-06'),
#         (2017, 27, 10100.00, 5.5, '2024-05-07', '2029-05-07'),
#         (2018, 28, 15500.00, 6.2, '2024-05-08', '2029-05-08'),
#         (2019, 29, 14300.00, 5.9, '2024-05-09', '2029-05-09'),
#         (2020, 30, 12800.00, 6.3, '2024-05-10', '2029-05-10'),
#         (2021, 31, 11000.00, 5.7, '2024-05-11', '2029-05-11'),
#         (2022, 32, 13500.00, 6.4, '2024-05-12', '2029-05-12'),
#         (2023, 33, 12300.00, 6.1, '2024-05-13', '2029-05-13'),
#         (2024, 34, 9600.00, 5.4, '2024-05-14', '2029-05-14'),
#         (2025, 35, 11200.00, 6.2, '2024-05-15', '2029-05-15'),
#         (2026, 36, 11900.00, 5.9, '2024-05-16', '2029-05-16'),
#         (2027, 37, 13800.00, 6.3, '2024-05-17', '2029-05-17'),
#         (2028, 38, 9800.00, 5.6, '2024-05-18', '2029-05-18'),
#         (2029, 39, 12700.00, 6.0, '2024-05-19', '2029-05-19'),
#         (2030, 40, 11500.00, 6.5, '2024-05-20', '2029-05-20');


# SELECT * FROM EXL_SCHEMA.Customers;
# SELECT * FROM EXL_SCHEMA.Accounts;
# SELECT * FROM EXL_SCHEMA.Transactions;
# SELECT * FROM EXL_SCHEMA.Loans;
