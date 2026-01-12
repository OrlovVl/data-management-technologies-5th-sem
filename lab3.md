```postgresql
-- Добавляем поле Week в существующую таблицу DimDate
ALTER TABLE DimDate ADD COLUMN Week INT;

-- Обновляем данные для добавления номера недели
UPDATE DimDate 
SET Week = EXTRACT(WEEK FROM Date);

-- Создаем индекс для оптимизации запросов по неделям
CREATE INDEX IX_DimDate_Week_Year ON DimDate(Week, Year);

CREATE TABLE FactWeeklySales (
    WeeklySalesKey SERIAL PRIMARY KEY,
    Week INT NOT NULL,
    Year INT NOT NULL,
    BranchKey INT NOT NULL,
    TotalSalesAmount DECIMAL(15,2) NOT NULL CHECK (TotalSalesAmount >= 0),
    TotalQuantity INT NOT NULL CHECK (TotalQuantity > 0),
    TotalDiscountAmount DECIMAL(15,2) NOT NULL CHECK (TotalDiscountAmount >= 0),
    AverageUnitPrice DECIMAL(10,2) NOT NULL CHECK (AverageUnitPrice >= 0),
    TransactionCount INT NOT NULL CHECK (TransactionCount > 0),
    rowguid UUID NOT NULL DEFAULT uuid_generate_v4(),
    LoadDate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE FactWeeklySales 
ADD CONSTRAINT FK_FactWeeklySales_DimBranch 
FOREIGN KEY (BranchKey) REFERENCES DimBranch(BranchKey);

CREATE INDEX IX_FactWeeklySales_Year_Week ON FactWeeklySales(Year, Week);
CREATE INDEX IX_FactWeeklySales_BranchKey ON FactWeeklySales(BranchKey);
```

```postgresql
INSERT INTO FactWeeklySales (
    Week, Year, BranchKey, TotalSalesAmount, TotalQuantity, 
    TotalDiscountAmount, AverageUnitPrice, TransactionCount
)
SELECT 
    dd.Week,
    dd.Year,
    fs.BranchKey,
    SUM(fs.LineTotalAmount) as TotalSalesAmount,
    SUM(fs.Quantity) as TotalQuantity,
    SUM(fs.DiscountAmount) as TotalDiscountAmount,
    AVG(fs.UnitPrice) as AverageUnitPrice,
    COUNT(DISTINCT fs.SaleKey) as TransactionCount
FROM FactSales fs
JOIN DimDate dd ON fs.DateKey = dd.DateKey
GROUP BY dd.Year, dd.Week, fs.BranchKey
ORDER BY dd.Year, dd.Week, fs.BranchKey;

-- Проверяем результаты
SELECT 
    'Weekly Sales Mart' as Mart_Name,
    COUNT(*) as Total_Weeks,
    SUM(TotalSalesAmount) as Total_Sales,
    SUM(TotalQuantity) as Total_Items,
    SUM(TransactionCount) as Total_Transactions
FROM FactWeeklySales;
```