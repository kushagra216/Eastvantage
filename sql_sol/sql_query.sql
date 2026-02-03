SELECT 
    c.customer_id as Customer,c.age as Age,i.item_name as Item,
    CAST(SUM(IFNULL(o.quantity, 0)) AS INTEGER) as Quantity
    FROM Customer c
    JOIN Sales s ON c.customer_id = s.customer_id
    JOIN Orders o ON s.sales_id = o.sales_id
    JOIN Items i ON o.item_id = i.item_id
    WHERE c.age BETWEEN 18 AND 35
    GROUP BY c.customer_id, i.item_name
    HAVING Quantity > 0
    ORDER BY c.customer_id, i.item_name;
