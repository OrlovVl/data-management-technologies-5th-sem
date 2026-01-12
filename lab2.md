
``` postgresql
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

  

CREATE TABLE DimCustomer (

    CustomerKey SERIAL PRIMARY KEY,

    CustomerID INT NOT NULL,

    LastName VARCHAR(255) NOT NULL,

    FirstName VARCHAR(255) NOT NULL,

    Patronymic VARCHAR(255),

    BranchName VARCHAR(50) NOT NULL,

    StartDate DATE NOT NULL,

    EndDate DATE,

    IsCurrent BOOLEAN DEFAULT TRUE,

    rowguid UUID NOT NULL DEFAULT uuid_generate_v4(),

    LoadDate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP

);

  

CREATE TABLE DimProduct (

    ProductKey SERIAL PRIMARY KEY,

    ProductID INT NOT NULL,

    ProductName VARCHAR(255) NOT NULL,

    StartDate DATE NOT NULL,

    EndDate DATE,

    IsCurrent BOOLEAN DEFAULT TRUE,

    rowguid UUID NOT NULL DEFAULT uuid_generate_v4(),

    LoadDate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP

);

  

CREATE TABLE DimCategory (

    CategoryKey SERIAL PRIMARY KEY,

    CategoryID INT NOT NULL,

    CategoryName VARCHAR(255) NOT NULL,

    StartDate DATE NOT NULL,

    EndDate DATE,

    IsCurrent BOOLEAN DEFAULT TRUE,

    rowguid UUID NOT NULL DEFAULT uuid_generate_v4(),

    LoadDate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP

);

  

CREATE TABLE BridgeProductCategory (

    ProductCategoryKey SERIAL PRIMARY KEY,

    ProductKey INT NOT NULL,

    CategoryKey INT NOT NULL,

    StartDate DATE NOT NULL,

    EndDate DATE,

    IsCurrent BOOLEAN DEFAULT TRUE,

    rowguid UUID NOT NULL DEFAULT uuid_generate_v4(),

    LoadDate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP

);

  

CREATE TABLE DimDate (

    DateKey SERIAL PRIMARY KEY,

    Date DATE NOT NULL UNIQUE,

    Day INT NOT NULL,

    Month INT NOT NULL,

    Year INT NOT NULL,

    Quarter INT NOT NULL,

    DayOfWeek INT NOT NULL,

    DayName VARCHAR(20) NOT NULL,

    MonthName VARCHAR(20) NOT NULL,

    IsWeekend BOOLEAN NOT NULL,

    rowguid UUID NOT NULL DEFAULT uuid_generate_v4(),

    LoadDate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP

);

  

CREATE TABLE DimBranch (

    BranchKey SERIAL PRIMARY KEY,

    BranchID INT NOT NULL,

    BranchName VARCHAR(50) NOT NULL,

    StartDate DATE NOT NULL,

    EndDate DATE,

    IsCurrent BOOLEAN DEFAULT TRUE,

    rowguid UUID NOT NULL DEFAULT uuid_generate_v4(),

    LoadDate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP

);

  

CREATE TABLE DimSale (

    SaleKey SERIAL PRIMARY KEY,

    SaleID INT NOT NULL,

    BranchName VARCHAR(50) NOT NULL,

    SaleDate TIMESTAMP NOT NULL,

    TotalAmount DECIMAL(10, 2) NOT NULL CHECK (TotalAmount >= 0),

    StartDate DATE NOT NULL,

    EndDate DATE,

    IsCurrent BOOLEAN DEFAULT TRUE,

    rowguid UUID NOT NULL DEFAULT uuid_generate_v4(),

    LoadDate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP

);

  

CREATE TABLE FactSales (

    SalesKey SERIAL PRIMARY KEY,

    SaleKey INT NOT NULL,

    CustomerKey INT NOT NULL,

    ProductKey INT NOT NULL,

    DateKey INT NOT NULL,

    BranchKey INT NOT NULL,

    Quantity INT NOT NULL CHECK (Quantity > 0),

    UnitPrice DECIMAL(10, 2) NOT NULL CHECK (UnitPrice >= 0),

    LineTotalAmount DECIMAL(10, 2) NOT NULL CHECK (LineTotalAmount >= 0),

    CatalogPrice DECIMAL(10, 2) NOT NULL CHECK (CatalogPrice >= 0),

    DiscountAmount DECIMAL(10, 2) NOT NULL CHECK (DiscountAmount >= 0),

    rowguid UUID NOT NULL DEFAULT uuid_generate_v4(),

    LoadDate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP

);

  

ALTER TABLE FactSales

ADD CONSTRAINT FK_FactSales_DimSale

FOREIGN KEY (SaleKey) REFERENCES DimSale(SaleKey);

  

ALTER TABLE FactSales

ADD CONSTRAINT FK_FactSales_DimCustomer

FOREIGN KEY (CustomerKey) REFERENCES DimCustomer(CustomerKey);

  

ALTER TABLE FactSales

ADD CONSTRAINT FK_FactSales_DimProduct

FOREIGN KEY (ProductKey) REFERENCES DimProduct(ProductKey);

  

ALTER TABLE FactSales

ADD CONSTRAINT FK_FactSales_DimDate

FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey);

  

ALTER TABLE FactSales

ADD CONSTRAINT FK_FactSales_DimBranch

FOREIGN KEY (BranchKey) REFERENCES DimBranch(BranchKey);

  

ALTER TABLE BridgeProductCategory

ADD CONSTRAINT FK_BridgeProductCategory_DimProduct

FOREIGN KEY (ProductKey) REFERENCES DimProduct(ProductKey);

  

ALTER TABLE BridgeProductCategory

ADD CONSTRAINT FK_BridgeProductCategory_DimCategory

FOREIGN KEY (CategoryKey) REFERENCES DimCategory(CategoryKey);

  

CREATE INDEX IX_FactSales_SaleKey ON FactSales(SaleKey);

CREATE INDEX IX_FactSales_DateKey ON FactSales(DateKey);

CREATE INDEX IX_FactSales_CustomerKey ON FactSales(CustomerKey);

CREATE INDEX IX_FactSales_ProductKey ON FactSales(ProductKey);

CREATE INDEX IX_FactSales_BranchKey ON FactSales(BranchKey);

CREATE INDEX IX_DimDate_Date ON DimDate(Date);

CREATE INDEX IX_DimCustomer_CustomerID ON DimCustomer(CustomerID);

CREATE INDEX IX_DimProduct_ProductID ON DimProduct(ProductID);

CREATE INDEX IX_DimSale_SaleID_Branch ON DimSale(SaleID, BranchName);

CREATE INDEX IX_BridgeProductCategory_ProductKey ON BridgeProductCategory(ProductKey);

CREATE INDEX IX_BridgeProductCategory_CategoryKey ON BridgeProductCategory(CategoryKey);
```

```postgresql
CREATE EXTENSION IF NOT EXISTS dblink;

  

INSERT INTO DimBranch (BranchID, BranchName, StartDate)

VALUES

(1, 'Филиал Запад', CURRENT_DATE),

(2, 'Филиал Восток', CURRENT_DATE);

  

INSERT INTO DimDate (Date, Day, Month, Year, Quarter, DayOfWeek, DayName, MonthName, IsWeekend)

SELECT

    datum AS Date,

    EXTRACT(DAY FROM datum) AS Day,

    EXTRACT(MONTH FROM datum) AS Month,

    EXTRACT(YEAR FROM datum) AS Year,

    EXTRACT(QUARTER FROM datum) AS Quarter,

    EXTRACT(ISODOW FROM datum) AS DayOfWeek,

    TO_CHAR(datum, 'Day') AS DayName,

    TO_CHAR(datum, 'Month') AS MonthName,

    EXTRACT(ISODOW FROM datum) IN (6, 7) AS IsWeekend

FROM (

    SELECT '2023-01-01'::DATE + SEQUENCE.DAY AS datum

    FROM GENERATE_SERIES(0, 1095) AS SEQUENCE(DAY)

) AS dates

ON CONFLICT (Date) DO NOTHING;

  

INSERT INTO DimCategory (CategoryID, CategoryName, StartDate)

SELECT

    pc.CategoryID,

    pc.CategoryName,

    CURRENT_DATE

FROM dblink('host=localhost dbname=west_branch user=postgres password=7410',

           'SELECT CategoryID, CategoryName FROM Category')

AS pc(CategoryID INT, CategoryName VARCHAR(255));

  

INSERT INTO DimProduct (ProductID, ProductName, StartDate)

SELECT

    pp.ProductID,

    pp.ProductName,

    CURRENT_DATE

FROM dblink('host=localhost dbname=west_branch user=postgres password=7410',

           'SELECT ProductID, ProductName FROM Product')

AS pp(ProductID INT, ProductName VARCHAR(255));

  

INSERT INTO DimCustomer (CustomerID, LastName, FirstName, Patronymic, BranchName, StartDate)

SELECT

    pc.CustomerID,

    pc.LastName,

    pc.FirstName,

    pc.Patronymic,

    'Филиал Запад' as BranchName,

    CURRENT_DATE

FROM dblink('host=localhost dbname=west_branch user=postgres password=7410',

           'SELECT CustomerID, LastName, FirstName, Patronymic FROM Customer')

AS pc(CustomerID INT, LastName VARCHAR(255), FirstName VARCHAR(255), Patronymic VARCHAR(255));

  

INSERT INTO DimCustomer (CustomerID, LastName, FirstName, Patronymic, BranchName, StartDate)

SELECT

    pc.CustomerID,

    pc.LastName,

    pc.FirstName,

    pc.Patronymic,

    'Филиал Восток' as BranchName,

    CURRENT_DATE

FROM dblink('host=localhost dbname=east_branch user=postgres password=7410',

           'SELECT CustomerID, LastName, FirstName, Patronymic FROM Customer')

AS pc(CustomerID INT, LastName VARCHAR(255), FirstName VARCHAR(255), Patronymic VARCHAR(255));

  

INSERT INTO BridgeProductCategory (ProductKey, CategoryKey, StartDate)

SELECT

    dp.ProductKey,

    dc.CategoryKey,

    CURRENT_DATE

FROM dblink('host=localhost dbname=west_branch user=postgres password=7410',

           'SELECT ProductID, CategoryID FROM ProductCategory')

AS pc(ProductID INT, CategoryID INT)

JOIN DimProduct dp ON pc.ProductID = dp.ProductID

JOIN DimCategory dc ON pc.CategoryID = dc.CategoryID;

  

INSERT INTO DimSale (SaleID, BranchName, SaleDate, TotalAmount, StartDate)

SELECT

    s.SaleID,

    'Филиал Запад' as BranchName,

    s.SaleDate,

    s.TotalAmount,

    CURRENT_DATE

FROM dblink('host=localhost dbname=west_branch user=postgres password=7410',

           'SELECT SaleID, SaleDate, TotalAmount FROM Sale')

AS s(SaleID INT, SaleDate TIMESTAMP, TotalAmount DECIMAL(10,2));

  

INSERT INTO DimSale (SaleID, BranchName, SaleDate, TotalAmount, StartDate)

SELECT

    s.SaleID,

    'Филиал Восток' as BranchName,

    s.SaleDate,

    s.TotalAmount,

    CURRENT_DATE

FROM dblink('host=localhost dbname=east_branch user=postgres password=7410',

           'SELECT SaleID, SaleDate, TotalAmount FROM Sale')

AS s(SaleID INT, SaleDate TIMESTAMP, TotalAmount DECIMAL(10,2));

  

INSERT INTO FactSales (SaleKey, CustomerKey, ProductKey, DateKey, BranchKey, Quantity, UnitPrice, LineTotalAmount, CatalogPrice, DiscountAmount)

SELECT

    ds.SaleKey,

    dc.CustomerKey,

    dp.ProductKey,

    dd.DateKey,

    (SELECT BranchKey FROM DimBranch WHERE BranchName = 'Филиал Запад') as BranchKey,

    fsd.Quantity,

    fsd.UnitPrice,

    fsd.Quantity * fsd.UnitPrice as LineTotalAmount,

    p_source.CatalogPrice,

    (p_source.CatalogPrice - fsd.UnitPrice) * fsd.Quantity as DiscountAmount

FROM dblink('host=localhost dbname=west_branch user=postgres password=7410',

           'SELECT s.SaleID, s.CustomerID, s.SaleDate, sd.ProductID, sd.Quantity, sd.UnitPrice

            FROM Sale s

            JOIN SaleDetail sd ON s.SaleID = sd.SaleID')

AS fsd(SaleID INT, CustomerID INT, SaleDate TIMESTAMP, ProductID INT, Quantity INT, UnitPrice DECIMAL(10,2))

JOIN dblink('host=localhost dbname=west_branch user=postgres password=7410',

           'SELECT ProductID, CatalogPrice FROM Product')

AS p_source(ProductID INT, CatalogPrice DECIMAL(10,2)) ON fsd.ProductID = p_source.ProductID

JOIN DimSale ds ON fsd.SaleID = ds.SaleID AND ds.BranchName = 'Филиал Запад'

JOIN DimCustomer dc ON fsd.CustomerID = dc.CustomerID AND dc.BranchName = 'Филиал Запад'

JOIN DimProduct dp ON fsd.ProductID = dp.ProductID

JOIN DimDate dd ON DATE(fsd.SaleDate) = dd.Date;

  

INSERT INTO FactSales (SaleKey, CustomerKey, ProductKey, DateKey, BranchKey, Quantity, UnitPrice, LineTotalAmount, CatalogPrice, DiscountAmount)

SELECT

    ds.SaleKey,

    dc.CustomerKey,

    dp.ProductKey,

    dd.DateKey,

    (SELECT BranchKey FROM DimBranch WHERE BranchName = 'Филиал Восток') as BranchKey,

    fsd.Quantity,

    fsd.UnitPrice,

    fsd.Quantity * fsd.UnitPrice as LineTotalAmount,

    p_source.CatalogPrice,

    (p_source.CatalogPrice - fsd.UnitPrice) * fsd.Quantity as DiscountAmount

FROM dblink('host=localhost dbname=east_branch user=postgres password=7410',

           'SELECT s.SaleID, s.CustomerID, s.SaleDate, sd.ProductID, sd.Quantity, sd.UnitPrice

            FROM Sale s

            JOIN SaleDetail sd ON s.SaleID = sd.SaleID')

AS fsd(SaleID INT, CustomerID INT, SaleDate TIMESTAMP, ProductID INT, Quantity INT, UnitPrice DECIMAL(10,2))

JOIN dblink('host=localhost dbname=west_branch user=postgres password=7410',

           'SELECT ProductID, CatalogPrice FROM Product')

AS p_source(ProductID INT, CatalogPrice DECIMAL(10,2)) ON fsd.ProductID = p_source.ProductID

JOIN DimSale ds ON fsd.SaleID = ds.SaleID AND ds.BranchName = 'Филиал Восток'

JOIN DimCustomer dc ON fsd.CustomerID = dc.CustomerID AND dc.BranchName = 'Филиал Восток'

JOIN DimProduct dp ON fsd.ProductID = dp.ProductID

JOIN DimDate dd ON DATE(fsd.SaleDate) = dd.Date;

  

-- Статистика загрузки

DO $$

DECLARE

    total_customers INT;

    total_products INT;

    total_categories INT;

    total_sales_headers INT;

    total_sales_details INT;

BEGIN

    SELECT COUNT(*) INTO total_customers FROM DimCustomer;

    SELECT COUNT(*) INTO total_products FROM DimProduct;

    SELECT COUNT(*) INTO total_categories FROM DimCategory;

    SELECT COUNT(*) INTO total_sales_headers FROM DimSale;

    SELECT COUNT(*) INTO total_sales_details FROM FactSales;

    RAISE NOTICE 'Загружено:';

    RAISE NOTICE '- Покупателей: %', total_customers;

    RAISE NOTICE '- Товаров: %', total_products;

    RAISE NOTICE '- Категорий: %', total_categories;

    RAISE NOTICE '- Чеков: %', total_sales_headers;

    RAISE NOTICE '- Позиций продаж: %', total_sales_details;

END $$;
```

```postgresql
TRUNCATE TABLE FactSales RESTART IDENTITY CASCADE;

TRUNCATE TABLE BridgeProductCategory RESTART IDENTITY CASCADE;

TRUNCATE TABLE DimCustomer RESTART IDENTITY CASCADE;

TRUNCATE TABLE DimProduct RESTART IDENTITY CASCADE;

TRUNCATE TABLE DimCategory RESTART IDENTITY CASCADE;

TRUNCATE TABLE DimBranch RESTART IDENTITY CASCADE;

  

SELECT

    'FactSales' as table_name, COUNT(*) as count FROM FactSales

UNION ALL

SELECT 'BridgeProductCategory', COUNT(*) FROM BridgeProductCategory

UNION ALL

SELECT 'DimCustomer', COUNT(*) FROM DimCustomer

UNION ALL

SELECT 'DimProduct', COUNT(*) FROM DimProduct

UNION ALL

SELECT 'DimCategory', COUNT(*) FROM DimCategory

UNION ALL

SELECT 'DimBranch', COUNT(*) FROM DimBranch;
```

```postgresql
    SELECT  dblink_connect('west_conn', 'host=localhost dbname=west_branch user=postgres password=7410');

    SELECT dblink_disconnect('west_conn');
```