```postgresql
-- 1. Создаем управляемую витрину недельных продаж (если не существует)

CREATE TABLE IF NOT EXISTS FactControlledWeeklySales (

    ControlledWeeklySalesKey SERIAL PRIMARY KEY,

    Year INT NOT NULL,

    Week INT NOT NULL,

    BranchKey INT NOT NULL,

    BranchName VARCHAR(50) NOT NULL,

    TotalSalesAmount DECIMAL(15,2) NOT NULL CHECK (TotalSalesAmount >= 0),

    TotalQuantity INT NOT NULL CHECK (TotalQuantity > 0),

    TotalDiscountAmount DECIMAL(15,2) NOT NULL CHECK (TotalDiscountAmount >= 0),

    AverageUnitPrice DECIMAL(10,2) NOT NULL CHECK (AverageUnitPrice >= 0),

    AverageTransactionValue DECIMAL(10,2) NOT NULL CHECK (AverageTransactionValue >= 0),

    TransactionCount INT NOT NULL CHECK (TransactionCount > 0),

    CustomerCount INT NOT NULL CHECK (CustomerCount > 0),

    AverageItemsPerTransaction DECIMAL(10,2) NOT NULL CHECK (AverageItemsPerTransaction >= 0),

    DiscountPercentage DECIMAL(5,2) NOT NULL CHECK (DiscountPercentage >= 0 AND DiscountPercentage <= 100),

    WeekStartDate DATE NOT NULL,

    WeekEndDate DATE NOT NULL,

    rowguid UUID NOT NULL DEFAULT uuid_generate_v4(),

    LoadDate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP

);

  

-- 2. Создаем индексы для оптимизации

CREATE INDEX IF NOT EXISTS IX_FactControlledWeeklySales_Year_Week

ON FactControlledWeeklySales(Year, Week);

  

CREATE INDEX IF NOT EXISTS IX_FactControlledWeeklySales_BranchKey

ON FactControlledWeeklySales(BranchKey);

  

CREATE INDEX IF NOT EXISTS IX_FactControlledWeeklySales_Dates

ON FactControlledWeeklySales(WeekStartDate, WeekEndDate);

  

-- 3. Добавляем внешний ключ (если не существует)

DO $$

BEGIN

    IF NOT EXISTS (SELECT 1 FROM information_schema.table_constraints

                  WHERE constraint_name = 'fk_factcontrolledweeklysales_dimbranch') THEN

        ALTER TABLE FactControlledWeeklySales

        ADD CONSTRAINT FK_FactControlledWeeklySales_DimBranch

        FOREIGN KEY (BranchKey) REFERENCES DimBranch(BranchKey);

    END IF;

END $$;

  

-- 4. Основная процедура обновления управляемой витрины

CREATE OR REPLACE PROCEDURE RefreshControlledWeeklySales(

    p_start_date DATE DEFAULT NULL,

    p_end_date DATE DEFAULT NULL,

    p_branch_name VARCHAR(50) DEFAULT NULL

)

LANGUAGE plpgsql

AS $$

DECLARE

    period_info TEXT;

    records_count INT;

    total_weeks INT;

    total_branches INT;

    total_sales DECIMAL(15,2);

    total_quantity INT;

    total_transactions INT;

BEGIN

    -- Всегда очищаем витрину полностью

    TRUNCATE TABLE FactControlledWeeklySales;

    RAISE NOTICE 'Управляемая витрина полностью очищена';

    -- Определяем период обновления

    IF p_start_date IS NULL OR p_end_date IS NULL THEN

        -- Полное обновление - все данные

        period_info := 'полное обновление (все данные)';

    ELSE

        -- Обновление за выбранный период

        period_info := 'обновление за период: ' || p_start_date || ' - ' || p_end_date;

    END IF;

    IF p_branch_name IS NOT NULL THEN

        period_info := period_info || ', филиал: ' || p_branch_name;

    END IF;

    RAISE NOTICE 'Начало обновления управляемой витрины. %', period_info;

    -- Убедимся, что поле Week существует в DimDate

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns

                  WHERE table_name = 'dimdate' AND column_name = 'week') THEN

        ALTER TABLE DimDate ADD COLUMN Week INT;

        UPDATE DimDate SET Week = EXTRACT(WEEK FROM Date);

        CREATE INDEX IF NOT EXISTS IX_DimDate_Week_Year ON DimDate(Week, Year);

        RAISE NOTICE 'Добавлено поле Week в DimDate';

    END IF;

    -- Наполняем управляемую витрину данными

    WITH weekly_data AS (

        SELECT

            dd.Year,

            dd.Week,

            fs.BranchKey,

            db.BranchName,

            MIN(dd.Date) as WeekStartDate,

            MAX(dd.Date) as WeekEndDate,

            SUM(fs.LineTotalAmount) as TotalSalesAmount,

            SUM(fs.Quantity) as TotalQuantity,

            SUM(fs.DiscountAmount) as TotalDiscountAmount,

            AVG(fs.UnitPrice) as AverageUnitPrice,

            COUNT(DISTINCT fs.SaleKey) as TransactionCount,

            COUNT(DISTINCT fs.CustomerKey) as CustomerCount

        FROM FactSales fs

        JOIN DimDate dd ON fs.DateKey = dd.DateKey

        JOIN DimBranch db ON fs.BranchKey = db.BranchKey

        WHERE

            (p_start_date IS NULL OR dd.Date BETWEEN p_start_date AND p_end_date)

            AND (p_branch_name IS NULL OR db.BranchName = p_branch_name)

        GROUP BY dd.Year, dd.Week, fs.BranchKey, db.BranchName

    ),

    inserted_data AS (

        INSERT INTO FactControlledWeeklySales (

            Year, Week, BranchKey, BranchName, TotalSalesAmount, TotalQuantity,

            TotalDiscountAmount, AverageUnitPrice, AverageTransactionValue,

            TransactionCount, CustomerCount, AverageItemsPerTransaction,

            DiscountPercentage, WeekStartDate, WeekEndDate

        )

        SELECT

            wd.Year,

            wd.Week,

            wd.BranchKey,

            wd.BranchName,

            wd.TotalSalesAmount,

            wd.TotalQuantity,

            wd.TotalDiscountAmount,

            wd.AverageUnitPrice,

            CASE

                WHEN wd.TransactionCount > 0 THEN wd.TotalSalesAmount / wd.TransactionCount

                ELSE 0

            END as AverageTransactionValue,

            wd.TransactionCount,

            wd.CustomerCount,

            CASE

                WHEN wd.TransactionCount > 0 THEN wd.TotalQuantity::DECIMAL / wd.TransactionCount

                ELSE 0

            END as AverageItemsPerTransaction,

            CASE

                WHEN wd.TotalSalesAmount > 0 THEN (wd.TotalDiscountAmount / wd.TotalSalesAmount) * 100

                ELSE 0

            END as DiscountPercentage,

            wd.WeekStartDate,

            wd.WeekEndDate

        FROM weekly_data wd

        ORDER BY wd.Year, wd.Week, wd.BranchName

        RETURNING 1

    )

    SELECT COUNT(*) INTO records_count FROM inserted_data;

    RAISE NOTICE 'Добавлено записей в управляемую витрину: %', records_count;

    -- Статистика по загруженным данным

    SELECT

        COUNT(DISTINCT Year || '-' || Week),

        COUNT(DISTINCT BranchKey),

        SUM(TotalSalesAmount),

        SUM(TotalQuantity),

        SUM(TransactionCount)

    INTO

        total_weeks,

        total_branches,

        total_sales,

        total_quantity,

        total_transactions

    FROM FactControlledWeeklySales;

    RAISE NOTICE 'Статистика управляемой витрины:';

    RAISE NOTICE '- Недель в витрине: %', total_weeks;

    RAISE NOTICE '- Филиалов: %', total_branches;

    RAISE NOTICE '- Общая сумма продаж: %', total_sales;

    RAISE NOTICE '- Общее количество товаров: %', total_quantity;

    RAISE NOTICE '- Общее количество транзакций: %', total_transactions;

    RAISE NOTICE 'Обновление управляемой витрины завершено успешно!';

EXCEPTION

    WHEN OTHERS THEN

        RAISE EXCEPTION 'Ошибка при обновлении управляемой витрины: %', SQLERRM;

END $$;

  
  
  
  
  
  
  

-- 5. Функция для детального просмотра данных управляемой витрины

CREATE OR REPLACE FUNCTION GetControlledWeeklySalesData(

    p_year INT DEFAULT NULL,

    p_week INT DEFAULT NULL,

    p_branch_name VARCHAR(50) DEFAULT NULL

)

RETURNS TABLE(

    BranchName VARCHAR(50),

    Year INT,

    Week INT,

    WeekPeriod TEXT,

    TotalSalesAmount DECIMAL(15,2),

    TotalQuantity INT,

    TotalDiscountAmount DECIMAL(15,2),

    AverageUnitPrice DECIMAL(10,2),

    AverageTransactionValue DECIMAL(10,2),

    TransactionCount INT,

    CustomerCount INT,

    AverageItemsPerTransaction DECIMAL(10,2),

    DiscountPercentage DECIMAL(5,2),

    SalesPerCustomer DECIMAL(10,2)

)

LANGUAGE plpgsql

AS $$

BEGIN

    RETURN QUERY

    SELECT

        fcws.BranchName,

        fcws.Year,

        fcws.Week,

        fcws.WeekStartDate::TEXT || ' - ' || fcws.WeekEndDate::TEXT as WeekPeriod,

        fcws.TotalSalesAmount,

        fcws.TotalQuantity,

        fcws.TotalDiscountAmount,

        fcws.AverageUnitPrice,

        fcws.AverageTransactionValue,

        fcws.TransactionCount,

        fcws.CustomerCount,

        fcws.AverageItemsPerTransaction,

        fcws.DiscountPercentage,

        CASE

            WHEN fcws.CustomerCount > 0 THEN fcws.TotalSalesAmount / fcws.CustomerCount

            ELSE 0

        END as SalesPerCustomer

    FROM FactControlledWeeklySales fcws

    WHERE (p_year IS NULL OR fcws.Year = p_year)

      AND (p_week IS NULL OR fcws.Week = p_week)

      AND (p_branch_name IS NULL OR fcws.BranchName = p_branch_name)

    ORDER BY fcws.Year DESC, fcws.Week DESC, fcws.BranchName;

END $$;

  

-- 6. Функция для получения сводной статистики по неделям

CREATE OR REPLACE FUNCTION GetControlledWeeklySalesStats()

RETURNS TABLE(

    Year INT,

    Week INT,

    WeekPeriod TEXT,

    TotalBranches BIGINT,

    TotalSalesAmount DECIMAL(15,2),

    TotalTransactions BIGINT,

    TotalCustomers BIGINT,

    AvgTransactionValue DECIMAL(10,2),

    AvgDiscountPercentage DECIMAL(5,2)

)

LANGUAGE plpgsql

AS $$

BEGIN

    RETURN QUERY

    SELECT

        fcws.Year,

        fcws.Week,

        MIN(fcws.WeekStartDate)::TEXT || ' - ' || MAX(fcws.WeekEndDate)::TEXT as WeekPeriod,

        COUNT(DISTINCT fcws.BranchKey) as TotalBranches,

        SUM(fcws.TotalSalesAmount) as TotalSalesAmount,

        SUM(fcws.TransactionCount) as TotalTransactions,

        SUM(fcws.CustomerCount) as TotalCustomers,

        AVG(fcws.AverageTransactionValue) as AvgTransactionValue,

        AVG(fcws.DiscountPercentage) as AvgDiscountPercentage

    FROM FactControlledWeeklySales fcws

    GROUP BY fcws.Year, fcws.Week

    ORDER BY fcws.Year DESC, fcws.Week DESC;

END $$;

  

-- 7. Функция для получения аналитики по филиалам

CREATE OR REPLACE FUNCTION GetBranchAnalytics(

    p_branch_name VARCHAR(50) DEFAULT NULL

)

RETURNS TABLE(

    BranchName VARCHAR(50),

    TotalWeeks BIGINT,

    TotalSalesAmount DECIMAL(15,2),

    TotalTransactions BIGINT,

    TotalCustomers BIGINT,

    AvgWeeklySales DECIMAL(15,2),

    AvgTransactionValue DECIMAL(10,2),

    AvgDiscountPercentage DECIMAL(5,2),

    CustomerRetentionRate DECIMAL(5,2)

)

LANGUAGE plpgsql

AS $$

BEGIN

    RETURN QUERY

    SELECT

        fcws.BranchName,

        COUNT(*) as TotalWeeks,

        SUM(fcws.TotalSalesAmount) as TotalSalesAmount,

        SUM(fcws.TransactionCount) as TotalTransactions,

        SUM(fcws.CustomerCount) as TotalCustomers,

        AVG(fcws.TotalSalesAmount) as AvgWeeklySales,

        AVG(fcws.AverageTransactionValue) as AvgTransactionValue,

        AVG(fcws.DiscountPercentage) as AvgDiscountPercentage,

        CASE

            WHEN SUM(fcws.TransactionCount) > 0 THEN

                (SUM(fcws.CustomerCount)::DECIMAL / SUM(fcws.TransactionCount)) * 100

            ELSE 0

        END as CustomerRetentionRate

    FROM FactControlledWeeklySales fcws

    WHERE (p_branch_name IS NULL OR fcws.BranchName = p_branch_name)

    GROUP BY fcws.BranchName

    ORDER BY TotalSalesAmount DESC;

END $$;

  

-- 8. Функция для получения информации об управляемой витрине

CREATE OR REPLACE FUNCTION GetControlledMartInfo()

RETURNS TABLE(

    InfoType TEXT,

    Description TEXT,

    Value TEXT

)

LANGUAGE plpgsql

AS $$

BEGIN

    -- Период данных

    RETURN QUERY

    SELECT

        'CONTROLLED_MART_INFO'::TEXT as InfoType,

        'Период данных'::TEXT as Description,

        COALESCE(

            (SELECT MIN(Year || '-W' || Week || ' (' || WeekStartDate || ')')

             FROM FactControlledWeeklySales) || ' - ' ||

            (SELECT MAX(Year || '-W' || Week || ' (' || WeekEndDate || ')')

             FROM FactControlledWeeklySales),

            'Нет данных'

        )::TEXT as Value;

    -- Количество записей

    RETURN QUERY

    SELECT

        'CONTROLLED_MART_INFO'::TEXT as InfoType,

        'Количество записей'::TEXT as Description,

        (SELECT COUNT(*)::TEXT FROM FactControlledWeeklySales) as Value;

    -- Филиалы

    RETURN QUERY

    SELECT

        'CONTROLLED_MART_INFO'::TEXT as InfoType,

        'Филиалы в витрине'::TEXT as Description,

        (SELECT COUNT(DISTINCT BranchKey)::TEXT FROM FactControlledWeeklySales) as Value;

    -- Общая статистика

    RETURN QUERY

    SELECT

        'CONTROLLED_MART_INFO'::TEXT as InfoType,

        'Общая сумма продаж'::TEXT as Description,

        (SELECT COALESCE(SUM(TotalSalesAmount)::TEXT, '0')

         FROM FactControlledWeeklySales) as Value;

    -- Средние показатели

    RETURN QUERY

    SELECT

        'CONTROLLED_MART_INFO'::TEXT as InfoType,

        'Средний чек по витрине'::TEXT as Description,

        (SELECT COALESCE(AVG(AverageTransactionValue)::TEXT, '0')

         FROM FactControlledWeeklySales) as Value;

END $$;
```

```postgresql
-- Полное обновление витрины (все данные)
CALL RefreshControlledWeeklySales();

-- Обновление только за конкретный период
CALL RefreshControlledWeeklySales('2023-01-01', '2023-03-31');

-- Обновление за период для конкретного филиала
CALL RefreshControlledWeeklySales('2023-01-01', '2023-03-31', 'Филиал Запад');

-- Просмотр результатов
SELECT * FROM GetControlledWeeklySalesData();
SELECT * FROM GetControlledWeeklySalesData(2023, 5, 'Филиал Запад');
SELECT * FROM GetControlledWeeklySalesStats();
SELECT * FROM GetBranchAnalytics();
SELECT * FROM GetControlledMartInfo();
```