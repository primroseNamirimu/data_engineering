CREATE TABLE employees (
"Employee_ID" INT PRIMARY KEY,
"Job_Title" VARCHAR(50),
"Store_ID" INT,
"Salary" DECIMAL(10, 2),
"Hire_Date" DATE,
"Email" VARCHAR(200),
"Phone" VARCHAR(20),
"Full_Name" VARCHAR(50)
);
CREATE TABLE customers (
"Customer_ID" INT PRIMARY KEY,
"Email" VARCHAR(100),
"Phone" VARCHAR(20),
"Address" TEXT,
"City" VARCHAR(50),
"Full_Name" VARCHAR(50)
);

CREATE TABLE products (
"Product_ID" INT PRIMARY KEY,
"Name" VARCHAR(100),
"Category" VARCHAR(50),
"Price" DECIMAL(10, 2),
"Stock_Quantity" INT
);

CREATE TABLE stores (
"Store_ID" INT PRIMARY KEY,
"Store_Name" VARCHAR(100),
"City" VARCHAR(50),
"State" VARCHAR(50),
"Country" VARCHAR(50)
);

CREATE TABLE orders (
"Order_ID" INT PRIMARY KEY,
"Customer_ID" INT,
"Order_Date" DATE,
"Store_ID" INT REFERENCES stores("Store_ID"),
"Total_Amount" DECIMAL(10, 2)
);

CREATE TABLE order_items (
"Order_Item_ID" SERIAL PRIMARY KEY,
"Order_ID" INT REFERENCES orders("Order_ID"),
"Product_ID" INT REFERENCES products("Product_ID"),
"Quantity" INT,
"Unit_Price" DECIMAL(10, 2)
);


-- QN 1 [Sales Perfomance By Location]

CREATE VIEW sales_performance_by_location AS 
	SELECT stores."Store_ID", stores."Store_Name", stores."City", stores."State", stores."Country", SUM(orders."Total_Amount") AS total_sales, COUNT(orders."Order_ID") AS orders_count 
	 FROM stores
	 INNER JOIN orders
	 ON stores."Store_ID"=orders."Store_ID"
	 GROUP BY stores."City", stores."Store_ID", stores."Store_Name", stores."State", stores."Country"
	 ORDER BY total_sales DESC;

-- QN 2 [Daily sales Overview]
CREATE VIEW daily_sales_overview AS
	SELECT "Order_Date",SUM("Total_Amount") AS total_daily_sales, COUNT("Order_ID") AS total_daily_orders FROM orders GROUP BY "Order_Date";

-- Qn 3 [Customer Order Summary] 
CREATE VIEW customer_order_summary AS
	SELECT customers."Customer_ID",customers."Full_Name",customers."Email",COUNT(orders."Order_ID") AS total_orders, SUM(orders."Total_Amount") AS total_amount_spent
	FROM customers
	JOIN orders
	ON customers."Customer_ID"=orders."Customer_ID"
	GROUP BY customers."Customer_ID",customers."Full_Name",customers."Email" ORDER BY total_orders DESC;
 
-- Qn 4 [Product Sales Summary]
CREATE VIEW product_sales_summary AS 
	SELECT products."Product_ID", products."Name", productS."Category", order_items."Quantity" AS total_quantity_sold, (order_items."Quantity" * order_items."Unit_Price" ) AS generated_revenue 
	FROM order_items
	JOIN products
	ON order_items."Product_ID"=products."Product_ID"
	ORDER BY total_quantity_sold DESC,generated_revenue DESC;

-- Qn 5 [LOW STOCK INVENTORY]
CREATE VIEW low_stock_product AS 
	SELECT "Product_ID", "Name", "Category", "Stock_Quantity" FROM products;

