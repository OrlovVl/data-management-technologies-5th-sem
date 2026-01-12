```postgresql
-- Создаем расширение dblink в филиале (если еще не создано)
CREATE EXTENSION IF NOT EXISTS dblink;

-- Основная процедура восстановления данных из хранилища
CREATE OR REPLACE PROCEDURE RestoreFromWarehouse(
    p_branch_name VARCHAR(50),
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
    warehouse_conn TEXT := 'host=localhost dbname=data_warehouse user=postgres password=7410';
    branch_key INT;
    records_count INT;
    total_categories INT := 0;
    total_products INT := 0;
    total_customers INT := 0;
    total_sales INT := 0;
    total_sale_details INT := 0;
    total_product_categories INT := 0;
BEGIN
    RAISE NOTICE 'Начало восстановления данных из хранилища для филиала: %', p_branch_name;
    RAISE NOTICE 'Период: % - %', 
        COALESCE(p_start_date::TEXT, 'все данные'), 
        COALESCE(p_end_date::TEXT, 'все данные');
    
    -- Получаем BranchKey для выбранного филиала из хранилища
    SELECT br.BranchKey INTO branch_key 
    FROM dblink(warehouse_conn, 
           'SELECT BranchKey, BranchName FROM DimBranch') 
    AS br(BranchKey INT, BranchName VARCHAR(50))
    WHERE br.BranchName = p_branch_name;
    
    IF branch_key IS NULL THEN
        RAISE EXCEPTION 'Филиал не найден в хранилище: %', p_branch_name;
    END IF;
    
    RAISE NOTICE 'Найден BranchKey: %', branch_key;
    
    -- 1. Восстановление категорий
    RAISE NOTICE 'Восстановление категорий...';
    WITH restored_categories AS (
        INSERT INTO Category (CategoryID, CategoryName, ModifiedDate)
        SELECT 
            dc.CategoryID,
            dc.CategoryName,
            CURRENT_TIMESTAMP
        FROM dblink(warehouse_conn,
               'SELECT CategoryID, CategoryName FROM DimCategory WHERE IsCurrent = TRUE') 
        AS dc(CategoryID INT, CategoryName VARCHAR(255))
        WHERE NOT EXISTS (
            SELECT 1 FROM Category c WHERE c.CategoryID = dc.CategoryID
        )
        RETURNING 1
    )
    SELECT COUNT(*) INTO total_categories FROM restored_categories;
    
    RAISE NOTICE 'Восстановлено категорий: %', total_categories;
    
    -- 2. Восстановление товаров
    RAISE NOTICE 'Восстановление товаров...';
    WITH restored_products AS (
        INSERT INTO Product (ProductID, ProductName, CatalogPrice, ModifiedDate)
        SELECT 
            dp.ProductID,
            dp.ProductName,
            -- Получаем актуальную цену из последней продажи товара
            COALESCE(
                (SELECT DISTINCT FIRST_VALUE(fs.CatalogPrice) OVER (
                    PARTITION BY fs.ProductKey 
                    ORDER BY dd.Date DESC
                 )
                 FROM dblink(warehouse_conn,
                        'SELECT fs.ProductKey, fs.CatalogPrice, dd.Date 
                         FROM FactSales fs 
                         JOIN DimDate dd ON fs.DateKey = dd.DateKey
                         WHERE fs.ProductKey = ' || dp.ProductKey || 
                         CASE WHEN p_start_date IS NOT NULL THEN 
                             ' AND dd.Date >= ''' || p_start_date || ''''
                         ELSE '' END ||
                         CASE WHEN p_end_date IS NOT NULL THEN 
                             ' AND dd.Date <= ''' || p_end_date || ''''
                         ELSE '' END
                 ) AS fs(ProductKey INT, CatalogPrice DECIMAL(10,2), Date DATE)
                 LIMIT 1),
                0
            ) as CatalogPrice,
            CURRENT_TIMESTAMP
        FROM dblink(warehouse_conn,
               'SELECT ProductID, ProductName FROM DimProduct WHERE IsCurrent = TRUE') 
        AS dp(ProductID INT, ProductName VARCHAR(255))
        WHERE EXISTS (
            SELECT 1 FROM dblink(warehouse_conn,
                   'SELECT DISTINCT dp.ProductID 
                    FROM FactSales fs 
                    JOIN DimProduct dp ON fs.ProductKey = dp.ProductKey 
                    JOIN DimDate dd ON fs.DateKey = dd.DateKey
                    WHERE fs.BranchKey = ' || branch_key ||
                    CASE WHEN p_start_date IS NOT NULL THEN 
                        ' AND dd.Date >= ''' || p_start_date || ''''
                    ELSE '' END ||
                    CASE WHEN p_end_date IS NOT NULL THEN 
                        ' AND dd.Date <= ''' || p_end_date || ''''
                    ELSE '' END
            ) AS active_products(ProductID INT)
            WHERE active_products.ProductID = dp.ProductID
        )
        AND NOT EXISTS (
            SELECT 1 FROM Product p WHERE p.ProductID = dp.ProductID
        )
        RETURNING 1
    )
    SELECT COUNT(*) INTO total_products FROM restored_products;
    
    RAISE NOTICE 'Восстановлено товаров: %', total_products;
    
    -- 3. Восстановление покупателей
    RAISE NOTICE 'Восстановление покупателей...';
    WITH restored_customers AS (
        INSERT INTO Customer (CustomerID, LastName, FirstName, Patronymic, ModifiedDate)
        SELECT DISTINCT
            dc.CustomerID,
            dc.LastName,
            dc.FirstName,
            dc.Patronymic,
            CURRENT_TIMESTAMP
        FROM dblink(warehouse_conn,
               'SELECT CustomerID, LastName, FirstName, Patronymic 
                FROM DimCustomer 
                WHERE BranchName = ''' || p_branch_name || ''' AND IsCurrent = TRUE') 
        AS dc(CustomerID INT, LastName VARCHAR(255), FirstName VARCHAR(255), Patronymic VARCHAR(255))
        WHERE EXISTS (
            SELECT 1 FROM dblink(warehouse_conn,
                   'SELECT DISTINCT fs.CustomerKey 
                    FROM FactSales fs 
                    JOIN DimDate dd ON fs.DateKey = dd.DateKey
                    WHERE fs.BranchKey = ' || branch_key ||
                    CASE WHEN p_start_date IS NOT NULL THEN 
                        ' AND dd.Date >= ''' || p_start_date || ''''
                    ELSE '' END ||
                    CASE WHEN p_end_date IS NOT NULL THEN 
                        ' AND dd.Date <= ''' || p_end_date || ''''
                    ELSE '' END
            ) AS active_customers(CustomerKey INT)
            WHERE active_customers.CustomerKey = dc.CustomerID
        )
        AND NOT EXISTS (
            SELECT 1 FROM Customer c WHERE c.CustomerID = dc.CustomerID
        )
        RETURNING 1
    )
    SELECT COUNT(*) INTO total_customers FROM restored_customers;
    
    RAISE NOTICE 'Восстановлено покупателей: %', total_customers;
    
    -- 4. Восстановление связей товаров и категорий
    RAISE NOTICE 'Восстановление связей товаров и категорий...';
    WITH restored_links AS (
        INSERT INTO ProductCategory (ProductID, CategoryID, ModifiedDate)
        SELECT DISTINCT
            dp.ProductID,
            dc.CategoryID,
            CURRENT_TIMESTAMP
        FROM dblink(warehouse_conn,
               'SELECT bpc.ProductKey, bpc.CategoryKey 
                FROM BridgeProductCategory bpc
                JOIN DimProduct dp ON bpc.ProductKey = dp.ProductKey
                JOIN DimCategory dc ON bpc.CategoryKey = dc.CategoryKey
                WHERE bpc.IsCurrent = TRUE') 
        AS bpc(ProductKey INT, CategoryKey INT)
        JOIN dblink(warehouse_conn,
               'SELECT ProductID, ProductKey FROM DimProduct') 
        AS dp(ProductID INT, ProductKey INT) ON bpc.ProductKey = dp.ProductKey
        JOIN dblink(warehouse_conn,
               'SELECT CategoryID, CategoryKey FROM DimCategory') 
        AS dc(CategoryID INT, CategoryKey INT) ON bpc.CategoryKey = dc.CategoryKey
        WHERE EXISTS (
            SELECT 1 FROM dblink(warehouse_conn,
                   'SELECT DISTINCT dp.ProductID 
                    FROM FactSales fs 
                    JOIN DimProduct dp ON fs.ProductKey = dp.ProductKey 
                    JOIN DimDate dd ON fs.DateKey = dd.DateKey
                    WHERE fs.BranchKey = ' || branch_key ||
                    CASE WHEN p_start_date IS NOT NULL THEN 
                        ' AND dd.Date >= ''' || p_start_date || ''''
                    ELSE '' END ||
                    CASE WHEN p_end_date IS NOT NULL THEN 
                        ' AND dd.Date <= ''' || p_end_date || ''''
                    ELSE '' END
            ) AS active_products(ProductID INT)
            WHERE active_products.ProductID = dp.ProductID
        )
        AND NOT EXISTS (
            SELECT 1 FROM ProductCategory pc 
            WHERE pc.ProductID = dp.ProductID AND pc.CategoryID = dc.CategoryID
        )
        RETURNING 1
    )
    SELECT COUNT(*) INTO total_product_categories FROM restored_links;
    
    RAISE NOTICE 'Восстановлено связей товаров и категорий: %', total_product_categories;
    
    -- 5. Восстановление продаж
    RAISE NOTICE 'Восстановление продаж...';
    WITH restored_sales AS (
        INSERT INTO Sale (SaleID, CustomerID, SaleDate, TotalAmount, ModifiedDate)
        SELECT DISTINCT
            ds.SaleID,
            dc.CustomerID,
            ds.SaleDate,
            ds.TotalAmount,
            CURRENT_TIMESTAMP
        FROM dblink(warehouse_conn,
               'SELECT SaleID, SaleDate, TotalAmount 
                FROM DimSale 
                WHERE BranchName = ''' || p_branch_name || ''' AND IsCurrent = TRUE') 
        AS ds(SaleID INT, SaleDate TIMESTAMP, TotalAmount DECIMAL(10,2))
        JOIN dblink(warehouse_conn,
               'SELECT CustomerID, CustomerKey FROM DimCustomer 
                WHERE BranchName = ''' || p_branch_name || '''') 
        AS dc(CustomerID INT, CustomerKey INT) ON EXISTS (
            SELECT 1 FROM dblink(warehouse_conn,
                   'SELECT fs.SaleKey, fs.CustomerKey 
                    FROM FactSales fs 
                    JOIN DimDate dd ON fs.DateKey = dd.DateKey
                    WHERE fs.BranchKey = ' || branch_key || 
                    ' AND fs.SaleKey = ' || ds.SaleID ||
                    CASE WHEN p_start_date IS NOT NULL THEN 
                        ' AND dd.Date >= ''' || p_start_date || ''''
                    ELSE '' END ||
                    CASE WHEN p_end_date IS NOT NULL THEN 
                        ' AND dd.Date <= ''' || p_end_date || ''''
                    ELSE '' END
            ) AS valid_sales(SaleKey INT, CustomerKey INT)
            WHERE valid_sales.CustomerKey = dc.CustomerKey
        )
        WHERE NOT EXISTS (
            SELECT 1 FROM Sale s WHERE s.SaleID = ds.SaleID
        )
        RETURNING 1
    )
    SELECT COUNT(*) INTO total_sales FROM restored_sales;
    
    RAISE NOTICE 'Восстановлено продаж: %', total_sales;
    
    -- 6. Восстановление деталей продаж
    RAISE NOTICE 'Восстановление деталей продаж...';
    WITH restored_sale_details AS (
        INSERT INTO SaleDetail (SaleID, ProductID, Quantity, UnitPrice, ModifiedDate)
        SELECT 
            ds.SaleID,
            dp.ProductID,
            fs.Quantity,
            fs.UnitPrice,
            CURRENT_TIMESTAMP
        FROM dblink(warehouse_conn,
               'SELECT fs.SaleKey, fs.ProductKey, fs.Quantity, fs.UnitPrice, dd.Date
                FROM FactSales fs
                JOIN DimDate dd ON fs.DateKey = dd.DateKey
                WHERE fs.BranchKey = ' || branch_key ||
                CASE WHEN p_start_date IS NOT NULL THEN 
                    ' AND dd.Date >= ''' || p_start_date || ''''
                ELSE '' END ||
                CASE WHEN p_end_date IS NOT NULL THEN 
                    ' AND dd.Date <= ''' || p_end_date || ''''
                ELSE '' END
        ) AS fs(SaleKey INT, ProductKey INT, Quantity INT, UnitPrice DECIMAL(10,2), Date DATE)
        JOIN dblink(warehouse_conn,
               'SELECT SaleID, SaleKey FROM DimSale 
                WHERE BranchName = ''' || p_branch_name || '''') 
        AS ds(SaleID INT, SaleKey INT) ON fs.SaleKey = ds.SaleKey
        JOIN dblink(warehouse_conn,
               'SELECT ProductID, ProductKey FROM DimProduct') 
        AS dp(ProductID INT, ProductKey INT) ON fs.ProductKey = dp.ProductKey
        WHERE NOT EXISTS (
            SELECT 1 FROM SaleDetail sd 
            WHERE sd.SaleID = ds.SaleID AND sd.ProductID = dp.ProductID
        )
        RETURNING 1
    )
    SELECT COUNT(*) INTO total_sale_details FROM restored_sale_details;
    
    RAISE NOTICE 'Восстановлено деталей продаж: %', total_sale_details;
    
    -- Итоговая статистика
    RAISE NOTICE '=== ВОССТАНОВЛЕНИЕ ИЗ ХРАНИЛИЩА ЗАВЕРШЕНО ===';
    RAISE NOTICE 'Филиал: %', p_branch_name;
    RAISE NOTICE 'Период: % - %', 
        COALESCE(p_start_date::TEXT, 'все данные'), 
        COALESCE(p_end_date::TEXT, 'все данные');
    RAISE NOTICE 'Восстановлено записей:';
    RAISE NOTICE '- Категорий: %', total_categories;
    RAISE NOTICE '- Товаров: %', total_products;
    RAISE NOTICE '- Покупателей: %', total_customers;
    RAISE NOTICE '- Связей товар-категория: %', total_product_categories;
    RAISE NOTICE '- Продаж: %', total_sales;
    RAISE NOTICE '- Деталей продаж: %', total_sale_details;
    
EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'Ошибка при восстановлении данных из хранилища: %', SQLERRM;
END $$;







-- Функция для проверки текущего состояния данных в филиале
CREATE OR REPLACE FUNCTION CheckLocalDataStatus()
RETURNS TABLE(
    TableName VARCHAR(50),
    RecordCount BIGINT,
    Status TEXT
) 
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        'Category' as TableName,
        COUNT(*) as RecordCount,
        CASE WHEN COUNT(*) > 0 THEN 'Данные присутствуют' ELSE 'Нет данных' END as Status
    FROM Category
    
    UNION ALL
    
    SELECT 
        'Product',
        COUNT(*),
        CASE WHEN COUNT(*) > 0 THEN 'Данные присутствуют' ELSE 'Нет данных' END
    FROM Product
    
    UNION ALL
    
    SELECT 
        'Customer',
        COUNT(*),
        CASE WHEN COUNT(*) > 0 THEN 'Данные присутствуют' ELSE 'Нет данных' END
    FROM Customer
    
    UNION ALL
    
    SELECT 
        'ProductCategory',
        COUNT(*),
        CASE WHEN COUNT(*) > 0 THEN 'Данные присутствуют' ELSE 'Нет данных' END
    FROM ProductCategory
    
    UNION ALL
    
    SELECT 
        'Sale',
        COUNT(*),
        CASE WHEN COUNT(*) > 0 THEN 'Данные присутствуют' ELSE 'Нет данных' END
    FROM Sale
    
    UNION ALL
    
    SELECT 
        'SaleDetail',
        COUNT(*),
        CASE WHEN COUNT(*) > 0 THEN 'Данные присутствуют' ELSE 'Нет данных' END
    FROM SaleDetail;
END $$;

-- Функция для проверки доступности хранилища и получения статистики
CREATE OR REPLACE FUNCTION CheckWarehouseAvailability(
    p_branch_name VARCHAR(50)
)
RETURNS TABLE(
    CheckType TEXT,
    Result TEXT,
    Details TEXT
) 
LANGUAGE plpgsql
AS $$
DECLARE
    warehouse_conn TEXT := 'host=localhost dbname=data_warehouse user=postgres password=7410';
    branch_key INT;
BEGIN
    -- Проверяем подключение к хранилищу
    BEGIN
        PERFORM dblink(warehouse_conn, 'SELECT 1');
        RETURN QUERY SELECT 'Подключение к хранилищу'::TEXT, 'УСПЕХ'::TEXT, 'Хранилище доступно'::TEXT;
    EXCEPTION
        WHEN OTHERS THEN
            RETURN QUERY SELECT 'Подключение к хранилищу'::TEXT, 'ОШИБКА'::TEXT, SQLERRM::TEXT;
            RETURN;
    END;
    
    -- Получаем BranchKey
    SELECT br.BranchKey INTO branch_key 
    FROM dblink(warehouse_conn, 
           'SELECT BranchKey, BranchName FROM DimBranch') 
    AS br(BranchKey INT, BranchName VARCHAR(50))
    WHERE br.BranchName = p_branch_name;
    
    IF branch_key IS NULL THEN
        RETURN QUERY SELECT 'Поиск филиала'::TEXT, 'ОШИБКА'::TEXT, 'Филиал не найден в хранилище'::TEXT;
        RETURN;
    ELSE
        RETURN QUERY SELECT 'Поиск филиала'::TEXT, 'УСПЕХ'::TEXT, ('BranchKey: ' || branch_key)::TEXT;
    END IF;
    
    -- Статистика по данным в хранилище
    RETURN QUERY 
    SELECT 
        'Данные в хранилище'::TEXT as CheckType,
        'СТАТИСТИКА'::TEXT as Result,
        ('Категории: ' || cat_count || ', Товары: ' || prod_count || ', Покупатели: ' || cust_count)::TEXT as Details
    FROM 
        (SELECT COUNT(*) as cat_count FROM dblink(warehouse_conn, 'SELECT CategoryKey FROM DimCategory WHERE IsCurrent = TRUE') AS t(k INT)) c,
        (SELECT COUNT(*) as prod_count FROM dblink(warehouse_conn, 'SELECT ProductKey FROM DimProduct WHERE IsCurrent = TRUE') AS t(k INT)) p,
        (SELECT COUNT(*) as cust_count FROM dblink(warehouse_conn, 'SELECT CustomerKey FROM DimCustomer WHERE BranchName = ''' || p_branch_name || ''' AND IsCurrent = TRUE') AS t(k INT)) u;
END $$;







-- Процедура для очистки всех данных в филиале
CREATE OR REPLACE PROCEDURE ClearLocalData()
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE 'Очистка всех данных филиала...';
    
    -- Очищаем в правильном порядке (с учетом внешних ключей)
    TRUNCATE TABLE SaleDetail, Sale, ProductCategory, Customer, Product, Category RESTART IDENTITY CASCADE;
    
    RAISE NOTICE 'Данные филиала полностью очищены';
    
    -- Проверяем результат
    PERFORM * FROM CheckLocalDataStatus();
END $$;
```

```postgresql

-- 2. Проверка текущего состояния данных в филиале
SELECT * FROM CheckLocalDataStatus();

-- 3. Полная очистка филиала (если нужно)
CALL ClearLocalData();

-- 4. Восстановление всех данных из хранилища
CALL RestoreFromWarehouse('Филиал Запад');

-- 6. Проверка результатов восстановления
SELECT * FROM CheckLocalDataStatus();
```
