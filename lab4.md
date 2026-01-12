```postgresql
-- Таблица для отслеживания состояния загрузки
CREATE TABLE IF NOT EXISTS ETL_LoadStatus (
    LoadStatusKey SERIAL PRIMARY KEY,
    BranchName VARCHAR(50) NOT NULL,
    TableName VARCHAR(50) NOT NULL,
    LastLoadDate TIMESTAMP NOT NULL,
    RecordsLoaded INT NOT NULL,
    LoadDate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    rowguid UUID NOT NULL DEFAULT uuid_generate_v4(),
    UNIQUE(BranchName, TableName)
);

-- Индекс для быстрого поиска
CREATE INDEX IF NOT EXISTS IX_ETL_LoadStatus_Branch_Table ON ETL_LoadStatus(BranchName, TableName);

-- Основная процедура инкрементальной загрузки
CREATE OR REPLACE PROCEDURE IncrementalLoadData()
LANGUAGE plpgsql
AS $$
DECLARE
    current_load_time TIMESTAMP := CURRENT_TIMESTAMP;
    last_load_time TIMESTAMP;
    records_count INT;
    branch_record RECORD;
    source_db VARCHAR;
    new_sale_ids TEXT := '';
BEGIN
    RAISE NOTICE 'Начало инкрементальной загрузки: %', current_load_time;
    
    -- Обрабатываем каждый филиал
    FOR branch_record IN 
        SELECT BranchKey, BranchName 
        FROM DimBranch 
        WHERE BranchName IN ('Филиал Запад', 'Филиал Восток')
    LOOP
        RAISE NOTICE 'Обработка филиала: %', branch_record.BranchName;
        
        -- Определяем базу-источник
        source_db := CASE 
            WHEN branch_record.BranchName = 'Филиал Запад' THEN 'west_branch'
            WHEN branch_record.BranchName = 'Филиал Восток' THEN 'east_branch'
        END;
        
        -- 1. Загрузка новых категорий
        RAISE NOTICE 'Загрузка категорий...';
        SELECT COALESCE(MAX(LastLoadDate), '1900-01-01'::TIMESTAMP)
        INTO last_load_time
        FROM ETL_LoadStatus 
        WHERE BranchName = branch_record.BranchName AND TableName = 'Category';
        
        WITH new_categories AS (
            INSERT INTO DimCategory (CategoryID, CategoryName, StartDate)
            SELECT 
                c.CategoryID,
                c.CategoryName,
                CURRENT_DATE
            FROM dblink(
                'host=localhost dbname=' || source_db || ' user=postgres password=7410',
                'SELECT CategoryID, CategoryName FROM Category WHERE ModifiedDate > ''' || last_load_time || ''''
            ) AS c(CategoryID INT, CategoryName VARCHAR(255))
            WHERE NOT EXISTS (
                SELECT 1 FROM DimCategory dc 
                WHERE dc.CategoryID = c.CategoryID AND dc.IsCurrent = TRUE
            )
            RETURNING 1
        )
        SELECT COUNT(*) INTO records_count FROM new_categories;
        
        INSERT INTO ETL_LoadStatus (BranchName, TableName, LastLoadDate, RecordsLoaded)
        VALUES (branch_record.BranchName, 'Category', current_load_time, records_count)
        ON CONFLICT (BranchName, TableName) 
        DO UPDATE SET LastLoadDate = current_load_time, RecordsLoaded = records_count;
        
        RAISE NOTICE 'Загружено категорий: %', records_count;
        
        -- 2. Загрузка новых товаров
        RAISE NOTICE 'Загрузка товаров...';
        SELECT COALESCE(MAX(LastLoadDate), '1900-01-01'::TIMESTAMP)
        INTO last_load_time
        FROM ETL_LoadStatus 
        WHERE BranchName = branch_record.BranchName AND TableName = 'Product';
        
        WITH new_products AS (
            INSERT INTO DimProduct (ProductID, ProductName, StartDate)
            SELECT 
                p.ProductID,
                p.ProductName,
                CURRENT_DATE
            FROM dblink(
                'host=localhost dbname=' || source_db || ' user=postgres password=7410',
                'SELECT ProductID, ProductName FROM Product WHERE ModifiedDate > ''' || last_load_time || ''''
            ) AS p(ProductID INT, ProductName VARCHAR(255))
            WHERE NOT EXISTS (
                SELECT 1 FROM DimProduct dp 
                WHERE dp.ProductID = p.ProductID AND dp.IsCurrent = TRUE
            )
            RETURNING 1
        )
        SELECT COUNT(*) INTO records_count FROM new_products;
        
        INSERT INTO ETL_LoadStatus (BranchName, TableName, LastLoadDate, RecordsLoaded)
        VALUES (branch_record.BranchName, 'Product', current_load_time, records_count)
        ON CONFLICT (BranchName, TableName) 
        DO UPDATE SET LastLoadDate = current_load_time, RecordsLoaded = records_count;
        
        RAISE NOTICE 'Загружено товаров: %', records_count;
        
        -- 3. Загрузка новых покупателей
        RAISE NOTICE 'Загрузка покупателей...';
        SELECT COALESCE(MAX(LastLoadDate), '1900-01-01'::TIMESTAMP)
        INTO last_load_time
        FROM ETL_LoadStatus 
        WHERE BranchName = branch_record.BranchName AND TableName = 'Customer';
        
        WITH new_customers AS (
            INSERT INTO DimCustomer (CustomerID, LastName, FirstName, Patronymic, BranchName, StartDate)
            SELECT 
                c.CustomerID,
                c.LastName,
                c.FirstName,
                c.Patronymic,
                branch_record.BranchName,
                CURRENT_DATE
            FROM dblink(
                'host=localhost dbname=' || source_db || ' user=postgres password=7410',
                'SELECT CustomerID, LastName, FirstName, Patronymic FROM Customer WHERE ModifiedDate > ''' || last_load_time || ''''
            ) AS c(CustomerID INT, LastName VARCHAR(255), FirstName VARCHAR(255), Patronymic VARCHAR(255))
            WHERE NOT EXISTS (
                SELECT 1 FROM DimCustomer dc 
                WHERE dc.CustomerID = c.CustomerID AND dc.BranchName = branch_record.BranchName AND dc.IsCurrent = TRUE
            )
            RETURNING 1
        )
        SELECT COUNT(*) INTO records_count FROM new_customers;
        
        INSERT INTO ETL_LoadStatus (BranchName, TableName, LastLoadDate, RecordsLoaded)
        VALUES (branch_record.BranchName, 'Customer', current_load_time, records_count)
        ON CONFLICT (BranchName, TableName) 
        DO UPDATE SET LastLoadDate = current_load_time, RecordsLoaded = records_count;
        
        RAISE NOTICE 'Загружено покупателей: %', records_count;
        
        -- 4. Загрузка новых связей товаров и категорий
        RAISE NOTICE 'Загрузка связей товаров и категорий...';
        SELECT COALESCE(MAX(LastLoadDate), '1900-01-01'::TIMESTAMP)
        INTO last_load_time
        FROM ETL_LoadStatus 
        WHERE BranchName = branch_record.BranchName AND TableName = 'ProductCategory';
        
        WITH new_links AS (
            INSERT INTO BridgeProductCategory (ProductKey, CategoryKey, StartDate)
            SELECT 
                dp.ProductKey,
                dc.CategoryKey,
                CURRENT_DATE
            FROM dblink(
                'host=localhost dbname=' || source_db || ' user=postgres password=7410',
                'SELECT ProductID, CategoryID FROM ProductCategory WHERE ModifiedDate > ''' || last_load_time || ''''
            ) AS pc(ProductID INT, CategoryID INT)
            JOIN DimProduct dp ON pc.ProductID = dp.ProductID
            JOIN DimCategory dc ON pc.CategoryID = dc.CategoryID
            WHERE NOT EXISTS (
                SELECT 1 FROM BridgeProductCategory bpc 
                WHERE bpc.ProductKey = dp.ProductKey AND bpc.CategoryKey = dc.CategoryKey AND bpc.IsCurrent = TRUE
            )
            RETURNING 1
        )
        SELECT COUNT(*) INTO records_count FROM new_links;
        
        INSERT INTO ETL_LoadStatus (BranchName, TableName, LastLoadDate, RecordsLoaded)
        VALUES (branch_record.BranchName, 'ProductCategory', current_load_time, records_count)
        ON CONFLICT (BranchName, TableName) 
        DO UPDATE SET LastLoadDate = current_load_time, RecordsLoaded = records_count;
        
        RAISE NOTICE 'Загружено связей: %', records_count;
        
        -- 5. Загрузка новых продаж (исправленная версия)
        RAISE NOTICE 'Загрузка продаж...';
        SELECT COALESCE(MAX(LastLoadDate), '1900-01-01'::TIMESTAMP)
        INTO last_load_time
        FROM ETL_LoadStatus 
        WHERE BranchName = branch_record.BranchName AND TableName = 'Sale';
        
        -- Вставляем заголовки продаж и получаем ID новых продаж
        WITH new_sales AS (
            INSERT INTO DimSale (SaleID, BranchName, SaleDate, TotalAmount, StartDate)
            SELECT 
                s.SaleID,
                branch_record.BranchName,
                s.SaleDate,
                s.TotalAmount,
                CURRENT_DATE
            FROM dblink(
                'host=localhost dbname=' || source_db || ' user=postgres password=7410',
                'SELECT SaleID, SaleDate, TotalAmount FROM Sale WHERE ModifiedDate > ''' || last_load_time || ''''
            ) AS s(SaleID INT, SaleDate TIMESTAMP, TotalAmount DECIMAL(10,2))
            WHERE NOT EXISTS (
                SELECT 1 FROM DimSale ds 
                WHERE ds.SaleID = s.SaleID AND ds.BranchName = branch_record.BranchName
            )
            RETURNING SaleKey, SaleID
        )
        -- Загружаем детали продаж для новых чеков
        INSERT INTO FactSales (SaleKey, CustomerKey, ProductKey, DateKey, BranchKey, Quantity, UnitPrice, LineTotalAmount, CatalogPrice, DiscountAmount)
        SELECT 
            ns.SaleKey,
            dc.CustomerKey,
            dp.ProductKey,
            dd.DateKey,
            db.BranchKey,
            sd.Quantity,
            sd.UnitPrice,
            sd.Quantity * sd.UnitPrice,
            p_source.CatalogPrice,
            (p_source.CatalogPrice - sd.UnitPrice) * sd.Quantity
        FROM new_sales ns
        JOIN dblink(
            'host=localhost dbname=' || source_db || ' user=postgres password=7410',
            'SELECT sd.SaleID, s.CustomerID, s.SaleDate, sd.ProductID, sd.Quantity, sd.UnitPrice 
             FROM SaleDetail sd
             JOIN Sale s ON s.SaleID = sd.SaleID 
             WHERE s.ModifiedDate > ''' || last_load_time || ''''
        ) AS sd(SaleID INT, CustomerID INT, SaleDate TIMESTAMP, ProductID INT, Quantity INT, UnitPrice DECIMAL(10,2))
            ON ns.SaleID = sd.SaleID
        JOIN dblink('host=localhost dbname=' || source_db || ' user=postgres password=7410',
               'SELECT ProductID, CatalogPrice FROM Product') 
            AS p_source(ProductID INT, CatalogPrice DECIMAL(10,2)) ON sd.ProductID = p_source.ProductID
        JOIN DimCustomer dc ON sd.CustomerID = dc.CustomerID AND dc.BranchName = branch_record.BranchName AND dc.IsCurrent = TRUE
        JOIN DimProduct dp ON sd.ProductID = dp.ProductID
        JOIN DimDate dd ON DATE(sd.SaleDate) = dd.Date
        JOIN DimBranch db ON db.BranchName = branch_record.BranchName;
        
        GET DIAGNOSTICS records_count = ROW_COUNT;
        
        -- Сохраняем количество загруженных продаж
        WITH sale_count AS (
            SELECT COUNT(*) as cnt
            FROM dblink(
                'host=localhost dbname=' || source_db || ' user=postgres password=7410',
                'SELECT SaleID FROM Sale WHERE ModifiedDate > ''' || last_load_time || ''''
            ) AS s(SaleID INT)
            WHERE NOT EXISTS (
                SELECT 1 FROM DimSale ds 
                WHERE ds.SaleID = s.SaleID AND ds.BranchName = branch_record.BranchName
            )
        )
        INSERT INTO ETL_LoadStatus (BranchName, TableName, LastLoadDate, RecordsLoaded)
        SELECT 
            branch_record.BranchName, 
            'Sale', 
            current_load_time, 
            COALESCE(cnt, 0)
        FROM sale_count
        ON CONFLICT (BranchName, TableName) 
        DO UPDATE SET LastLoadDate = current_load_time, RecordsLoaded = EXCLUDED.RecordsLoaded;
        
        RAISE NOTICE 'Загружено позиций продаж: %', records_count;
        
    END LOOP;
    
    -- 6. Обновление витрины недельных продаж
    RAISE NOTICE 'Обновление витрины недельных продаж...';
    
    -- Добавляем поле Week в DimDate если его нет
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                  WHERE table_name = 'dimdate' AND column_name = 'week') THEN
        ALTER TABLE DimDate ADD COLUMN Week INT;
        UPDATE DimDate SET Week = EXTRACT(WEEK FROM Date);
        CREATE INDEX IF NOT EXISTS IX_DimDate_Week_Year ON DimDate(Week, Year);
    END IF;
    
    -- Создаем таблицу для витрины если ее нет
    CREATE TABLE IF NOT EXISTS FactWeeklySales (
        WeeklySalesKey SERIAL PRIMARY KEY,
        Year INT NOT NULL,
        Week INT NOT NULL,
        BranchKey INT NOT NULL,
        TotalSalesAmount DECIMAL(15,2) NOT NULL CHECK (TotalSalesAmount >= 0),
        TotalQuantity INT NOT NULL CHECK (TotalQuantity > 0),
        TotalDiscountAmount DECIMAL(15,2) NOT NULL CHECK (TotalDiscountAmount >= 0),
        AverageUnitPrice DECIMAL(10,2) NOT NULL CHECK (AverageUnitPrice >= 0),
        TransactionCount INT NOT NULL CHECK (TransactionCount > 0),
        rowguid UUID NOT NULL DEFAULT uuid_generate_v4(),
        LoadDate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT FK_FactWeeklySales_DimBranch FOREIGN KEY (BranchKey) REFERENCES DimBranch(BranchKey)
    );
    
    CREATE INDEX IF NOT EXISTS IX_FactWeeklySales_Year_Week ON FactWeeklySales(Year, Week);
    CREATE INDEX IF NOT EXISTS IX_FactWeeklySales_BranchKey ON FactWeeklySales(BranchKey);
    
    -- Очищаем и перезаполняем витрину
    TRUNCATE TABLE FactWeeklySales;
    
    WITH weekly_stats AS (
        INSERT INTO FactWeeklySales (Year, Week, BranchKey, TotalSalesAmount, TotalQuantity, TotalDiscountAmount, AverageUnitPrice, TransactionCount)
        SELECT 
            dd.Year,
            dd.Week,
            fs.BranchKey,
            SUM(fs.LineTotalAmount) as TotalSalesAmount,
            SUM(fs.Quantity) as TotalQuantity,
            SUM(fs.DiscountAmount) as TotalDiscountAmount,
            AVG(fs.UnitPrice) as AverageUnitPrice,
            COUNT(DISTINCT fs.SaleKey) as TransactionCount
        FROM FactSales fs
        JOIN DimDate dd ON fs.DateKey = dd.DateKey
        GROUP BY dd.Year, dd.Week, fs.BranchKey
        RETURNING 1
    )
    SELECT COUNT(*) INTO records_count FROM weekly_stats;
    
    RAISE NOTICE 'Обновлено записей в витрине: %', records_count;
    RAISE NOTICE 'Инкрементальная загрузка завершена успешно!';
    
EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'Ошибка при инкрементальной загрузке: %', SQLERRM;
END $$;

-- Вспомогательная функция статуса
CREATE OR REPLACE FUNCTION GetLoadStatus()
RETURNS TABLE(
    BranchName VARCHAR,
    TableName VARCHAR, 
    LastLoadDate TIMESTAMP,
    RecordsLoaded INT,
    LoadDate TIMESTAMP
) 
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        ls.BranchName,
        ls.TableName,
        ls.LastLoadDate,
        ls.RecordsLoaded,
        ls.LoadDate
    FROM ETL_LoadStatus ls
    ORDER BY ls.BranchName, ls.TableName;
END $$;
```

```postgresql
-- Выполнение инкрементальной загрузки
CALL IncrementalLoadData();

-- Проверка статуса загрузки
SELECT * FROM GetLoadStatus();

-- Статистика по загрузкам
SELECT 
    BranchName,
    COUNT(*) as TotalLoads,
    SUM(RecordsLoaded) as TotalRecords
FROM ETL_LoadStatus 
GROUP BY BranchName;

```

```postgresql
-- Тестовые данные для Филиала Запад  west_branch
INSERT INTO Customer (LastName, FirstName, Patronymic, ModifiedDate)
VALUES 
('Петров', 'Алексей', 'Викторович', NOW()),
('Семенова', 'Ольга', 'Игоревна', NOW());

INSERT INTO Product (ProductName, CatalogPrice, ModifiedDate)
VALUES 
('Тестовый товар 1', 1500.00, NOW()),
('Тестовый товар 2', 2750.50, NOW());

INSERT INTO Category (CategoryName, ModifiedDate)
VALUES 
('Тестовая категория A', NOW()),
('Тестовая категория B', NOW());

-- Связываем товары с категориями
INSERT INTO ProductCategory (ProductID, CategoryID, ModifiedDate)
SELECT 
    (SELECT MAX(ProductID) FROM Product) - 1,
    (SELECT MAX(CategoryID) FROM Category) - 1,
    NOW();

INSERT INTO ProductCategory (ProductID, CategoryID, ModifiedDate)
SELECT 
    (SELECT MAX(ProductID) FROM Product),
    (SELECT MAX(CategoryID) FROM Category),
    NOW();

-- Добавляем продажи
INSERT INTO Sale (CustomerID, SaleDate, TotalAmount, ModifiedDate)
SELECT 
    (SELECT MAX(CustomerID) FROM Customer) - 1,
    NOW() - INTERVAL '1 day',
    4500.00,
    NOW();

INSERT INTO SaleDetail (SaleID, ProductID, Quantity, UnitPrice, ModifiedDate)
SELECT 
    (SELECT MAX(SaleID) FROM Sale),
    (SELECT MAX(ProductID) FROM Product) - 1,
    2,
    1500.00,
    NOW();

INSERT INTO SaleDetail (SaleID, ProductID, Quantity, UnitPrice, ModifiedDate)
SELECT 
    (SELECT MAX(SaleID) FROM Sale),
    (SELECT MAX(ProductID) FROM Product),
    1,
    1500.00,
    NOW();
```

```postgresql
-- Тестовые данные для Филиала Восток east_branch
INSERT INTO Customer (LastName, FirstName, Patronymic, ModifiedDate)
VALUES 
('Ким', 'Чен', 'Су', NOW()),
('Танака', 'Юки', '', NOW());

INSERT INTO Product (ProductName, CatalogPrice, ModifiedDate)
VALUES 
('Восточный товар X', 3200.00, NOW()),
('Восточный товар Y', 1850.75, NOW());

INSERT INTO Category (CategoryName, ModifiedDate)
VALUES 
('Восточная категория 1', NOW()),
('Восточная категория 2', NOW());

-- Связываем товары с категориями
INSERT INTO ProductCategory (ProductID, CategoryID, ModifiedDate)
SELECT 
    (SELECT MAX(ProductID) FROM Product) - 1,
    (SELECT MAX(CategoryID) FROM Category) - 1,
    NOW();

INSERT INTO ProductCategory (ProductID, CategoryID, ModifiedDate)
SELECT 
    (SELECT MAX(ProductID) FROM Product),
    (SELECT MAX(CategoryID) FROM Category),
    NOW();

-- Добавляем продажи
INSERT INTO Sale (CustomerID, SaleDate, TotalAmount, ModifiedDate)
SELECT 
    (SELECT MAX(CustomerID) FROM Customer),
    NOW() - INTERVAL '2 hours',
    5050.75,
    NOW();

INSERT INTO SaleDetail (SaleID, ProductID, Quantity, UnitPrice, ModifiedDate)
SELECT 
    (SELECT MAX(SaleID) FROM Sale),
    (SELECT MAX(ProductID) FROM Product) - 1,
    1,
    3200.00,
    NOW();

INSERT INTO SaleDetail (SaleID, ProductID, Quantity, UnitPrice, ModifiedDate)
SELECT 
    (SELECT MAX(SaleID) FROM Sale),
    (SELECT MAX(ProductID) FROM Product),
    1,
    1850.75,
    NOW();
```

```postgresql
-- Проверим текущее состояние хранилища перед загрузкой
SELECT 'Текущее состояние хранилища:' as info;

SELECT 'Количество покупателей:' as metric, COUNT(*) as count FROM DimCustomer
UNION ALL
SELECT 'Количество товаров:', COUNT(*) FROM DimProduct
UNION ALL
SELECT 'Количество категорий:', COUNT(*) FROM DimCategory
UNION ALL
SELECT 'Количество связей:', COUNT(*) FROM BridgeProductCategory
UNION ALL
SELECT 'Количество чеков:', COUNT(*) FROM DimSale
UNION ALL
SELECT 'Количество позиций продаж:', COUNT(*) FROM FactSales
UNION ALL
SELECT 'Количество записей в витрине:', COUNT(*) FROM FactWeeklySales;
```

```postgresql
-- Запускаем процедуру инкрементальной загрузки
CALL IncrementalLoadData();
```

```postgresql
-- Проверим статус загрузки
SELECT * FROM GetLoadStatus() ORDER BY BranchName, TableName;

-- Проверим новые данные в хранилище
SELECT 'Состояние хранилища после загрузки:' as info;

SELECT 'Количество покупателей:' as metric, COUNT(*) as count FROM DimCustomer
UNION ALL
SELECT 'Количество товаров:', COUNT(*) FROM DimProduct
UNION ALL
SELECT 'Количество категорий:', COUNT(*) FROM DimCategory
UNION ALL
SELECT 'Количество связей:', COUNT(*) FROM BridgeProductCategory
UNION ALL
SELECT 'Количество чеков:', COUNT(*) FROM DimSale
UNION ALL
SELECT 'Количество позиций продаж:', COUNT(*) FROM FactSales
UNION ALL
SELECT 'Количество записей в витрине:', COUNT(*) FROM FactWeeklySales;

-- Проверим конкретно новые записи
SELECT 'Новые покупатели:' as info;
SELECT * FROM DimCustomer 
WHERE LastName IN ('Петров', 'Семенова', 'Ким', 'Танака')
ORDER BY BranchName, LastName;

SELECT 'Новые товары:' as info;
SELECT * FROM DimProduct 
WHERE ProductName LIKE '%Тестовый%' OR ProductName LIKE '%Восточный%'
ORDER BY ProductName;

SELECT 'Новые категории:' as info;
SELECT * FROM DimCategory 
WHERE CategoryName LIKE '%Тестовая%' OR CategoryName LIKE '%Восточная%'
ORDER BY CategoryName;

SELECT 'Новые продажи:' as info;
SELECT 
    ds.SaleID,
    ds.BranchName,
    ds.SaleDate,
    ds.TotalAmount,
    dc.LastName,
    dc.FirstName
FROM DimSale ds
JOIN DimCustomer dc ON ds.BranchName = dc.BranchName 
WHERE ds.TotalAmount IN (4500.00, 5050.75)
ORDER BY ds.BranchName, ds.SaleDate;

-- Проверим витрину недельных продаж
SELECT 'Витрина недельных продаж:' as info;
SELECT 
    db.BranchName,
    fws.Year,
    fws.Week,
    fws.TotalSalesAmount,
    fws.TotalQuantity,
    fws.TransactionCount
FROM FactWeeklySales fws
JOIN DimBranch db ON fws.BranchKey = db.BranchKey
WHERE fws.TotalSalesAmount > 0
ORDER BY fws.Year DESC, fws.Week DESC, db.BranchName;
```

```postgresql
-- Повторно запустим процедуру, чтобы убедиться, что дублирования нет
SELECT 'Повторный запуск инкрементальной загрузки...' as info;
CALL IncrementalLoadData();

-- Проверим, что количество записей не изменилось
SELECT 'Проверка отсутствия дублирования:' as info;

SELECT 'Количество покупателей после повторной загрузки:' as metric, COUNT(*) as count FROM DimCustomer
UNION ALL
SELECT 'Количество товаров после повторной загрузки:', COUNT(*) FROM DimProduct
UNION ALL
SELECT 'Количество категорий после повторной загрузки:', COUNT(*) FROM DimCategory
UNION ALL
SELECT 'Количество чеков после повторной загрузки:', COUNT(*) FROM DimSale
UNION ALL
SELECT 'Количество позиций продаж после повторной загрузки:', COUNT(*) FROM FactSales;

-- Проверим статус загрузки - RecordsLoaded должно быть 0 для всех таблиц
SELECT 'Статус повторной загрузки (все RecordsLoaded должны быть 0):' as info;
SELECT * FROM GetLoadStatus() ORDER BY BranchName, TableName;
```

```postgresql
-- Обновим некоторые записи в источниках и проверим загрузку изменений
UPDATE west_branch.Customer 
SET LastName = 'Петров-Обновленный', ModifiedDate = NOW()
WHERE LastName = 'Петров';

UPDATE east_branch.Product 
SET CatalogPrice = 3500.00, ModifiedDate = NOW()
WHERE ProductName = 'Восточный товар X';

-- Добавим еще одну продажу
INSERT INTO west_branch.Sale (CustomerID, SaleDate, TotalAmount, ModifiedDate)
SELECT 
    (SELECT MAX(CustomerID) FROM west_branch.Customer),
    NOW(),
    2750.50,
    NOW();

INSERT INTO west_branch.SaleDetail (SaleID, ProductID, Quantity, UnitPrice, ModifiedDate)
SELECT 
    (SELECT MAX(SaleID) FROM west_branch.Sale),
    (SELECT MAX(ProductID) FROM west_branch.Product),
    1,
    2750.50,
    NOW();

-- Запустим загрузку снова
SELECT 'Загрузка после обновлений...' as info;
CALL IncrementalLoadData();

-- Проверим результаты
SELECT 'Результаты после обновлений:' as info;
SELECT * FROM GetLoadStatus() WHERE RecordsLoaded > 0 ORDER BY BranchName, TableName;

-- Проверим, что появились новые записи
SELECT 'Новые данные после обновлений:' as info;
SELECT 'Количество чеков:' as metric, COUNT(*) as count FROM DimSale
UNION ALL
SELECT 'Количество позиций продаж:', COUNT(*) FROM FactSales;
```

```postgresql
-- Финальная проверка всех данных
DO $$
DECLARE
    total_customers INT;
    total_products INT;
    total_categories INT;
    total_sales INT;
    total_facts INT;
    total_weekly INT;
BEGIN
    SELECT COUNT(*) INTO total_customers FROM DimCustomer;
    SELECT COUNT(*) INTO total_products FROM DimProduct;
    SELECT COUNT(*) INTO total_categories FROM DimCategory;
    SELECT COUNT(*) INTO total_sales FROM DimSale;
    SELECT COUNT(*) INTO total_facts FROM FactSales;
    SELECT COUNT(*) INTO total_weekly FROM FactWeeklySales;
    
    RAISE NOTICE 'ИТОГОВАЯ СТАТИСТИКА:';
    RAISE NOTICE '- Покупателей: %', total_customers;
    RAISE NOTICE '- Товаров: %', total_products;
    RAISE NOTICE '- Категорий: %', total_categories;
    RAISE NOTICE '- Чеков: %', total_sales;
    RAISE NOTICE '- Позиций продаж: %', total_facts;
    RAISE NOTICE '- Записей в витрине: %', total_weekly;
    
    -- Проверим, что данные корректно связаны
    IF EXISTS (
        SELECT 1 FROM FactSales fs 
        WHERE NOT EXISTS (SELECT 1 FROM DimCustomer dc WHERE dc.CustomerKey = fs.CustomerKey)
        OR NOT EXISTS (SELECT 1 FROM DimProduct dp WHERE dp.ProductKey = fs.ProductKey)
        OR NOT EXISTS (SELECT 1 FROM DimDate dd WHERE dd.DateKey = fs.DateKey)
        OR NOT EXISTS (SELECT 1 FROM DimBranch db WHERE db.BranchKey = fs.BranchKey)
    ) THEN
        RAISE NOTICE 'ВНИМАНИЕ: Найдены несвязанные записи в FactSales';
    ELSE
        RAISE NOTICE '✓ Все записи в FactSales корректно связаны';
    END IF;
    
    RAISE NOTICE '✓ Тестирование завершено успешно!';
END $$;
```