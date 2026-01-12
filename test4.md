```postgresql
CALL ClearLocalData();



-- Наполнение
INSERT INTO Customer (LastName, FirstName, Patronymic, ModifiedDate)
VALUES 
('Петров', 'Алексей', 'Викторович', NOW()),
('Семенова', 'Ольга', 'Игоревна', NOW());

INSERT INTO Product (ProductName, CatalogPrice, ModifiedDate)
VALUES 
('Тестовый товар 1', 1500.00, NOW()),
('Тестовый товар 2', 2750.50, NOW()),
('Тестовый товар 3', 3200.00, NOW());

INSERT INTO Category (CategoryName, ModifiedDate)
VALUES 
('Тестовая категория A', NOW()),
('Тестовая категория B', NOW());

INSERT INTO ProductCategory (ProductID, CategoryID, ModifiedDate)
VALUES 
(1, 1, NOW()),
(2, 2, NOW()),
(3, 1, NOW());

-- Первый чек (Петров: товары 1 и 2)
INSERT INTO Sale (CustomerID, SaleDate, TotalAmount, ModifiedDate)
VALUES (1, NOW(), 4250.50, NOW());

INSERT INTO SaleDetail (SaleID, ProductID, Quantity, UnitPrice, ModifiedDate)
VALUES 
(1, 1, 1, 1500.00, NOW()),
(1, 2, 1, 2750.50, NOW());




-- Перенос в хранилище, с выводом Fact
CALL IncrementalLoadData();

SELECT * FROM FactSales



-- Удаление
DELETE FROM SaleDetail WHERE SaleID = 1;
DELETE FROM Sale WHERE SaleID = 1;



-- Второй чек (Семенова: товары 2 и 3)
INSERT INTO Sale (CustomerID, SaleDate, TotalAmount, ModifiedDate)
VALUES (2, NOW(), 5950.50, NOW());

INSERT INTO SaleDetail (SaleID, ProductID, Quantity, UnitPrice, ModifiedDate)
VALUES 
(2, 2, 1, 2750.50, NOW()),
(2, 3, 1, 3200.00, NOW());


-- Перенос в хранилище, с выводом Fact
CALL IncrementalLoadData();

SELECT * FROM FactSales
```
