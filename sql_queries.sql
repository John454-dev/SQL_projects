use school;

select * from customers;
select * from orders;

-- 1. Find each customer’s latest and earliest order date using window functions.
select distinct c.customer_name, min(o.order_date) over(partition by c.customer_name) as earliest_order, max(o.order_date) over(partition by c.customer_name) as latest_order
from customers c
join orders o
on c.customer_id=o.customer_id;

-- 2. Show each order along with the previous and next order amounts for that customer.
select c.customer_id, o.order_id, o.order_amount, lag(o.order_amount) over(partition by c.customer_id order by o.order_amount) as prev_amount, 
lead(o.order_amount) over(partition by c.customer_id order by o.order_amount) as next_amount
from customers c 
join orders o 
on c.customer_id=o.customer_id;

-- 3. Rank all orders by amount for each customer.
select customer_id, order_amount, rank() over(partition by customer_id order by order_amount) from orders;

-- 4. Find the cumulative (running) total order amount for each customer ordered by date.
select customer_id, order_date, sum(order_amount) over(partition by customer_id order by order_date) as running_total from orders
order by customer_id, order_date;

-- 5. Display the percentage contribution of each order to the customer’s total spending.
select customer_id, order_amount, sum(order_amount) over(partition by customer_id), order_amount * 100 / sum(order_amount) over(partition by customer_id) as per_cont 
from orders
order by customer_id;

-- 6. Show the difference between each order and the average order amount for that customer.
select customer_id, order_amount, avg(order_amount) over(partition by customer_id) as avg_amnt, order_amount-avg(order_amount) over(partition by customer_id) as diff_amnt
from orders
order by customer_id;

-- 7. Find each customer’s order rank within their city based on order amount.
select c.customer_name, c.city, o.order_id, o.order_amount, rank() over(partition by c.city order by o.order_amount desc) as ranks from customers c
join orders o 
on c.customer_id = o.customer_id;

-- 8. Show the top 3 most expensive orders per customer.
select * from (
select customer_id, order_id, order_amount, row_number() over(partition by customer_id order by order_amount desc) as rnk
from orders
) rnked
where rnk<=3;

-- 9. Find customers whose latest order status is ‘Cancelled’.
select * from (
select customer_id, order_date, status, row_number() over(order by order_date desc) as latest_date from orders
) lts_orders
where status="Cancelled";

-- 10. Rank all cities by total revenue using window functions (no GROUP BY).
select distinct c.city, sum(o.order_amount) over(partition by c.city) as total_rev, rank() over(order by c.city desc) from customers c
join orders o
on c.customer_id = o.customer_id;

-- 11. Show for each segment (SMB, Individual) the rank of customers by their total spend.
select c.customer_name, c.segment, sum(o.order_amount) over(partition by c.customer_name) as total_amnt, rank() over(order by c.segment) as rnk
from customers c
join orders o 
on c.customer_id = o.customer_id
where c.segment in ("SMB", "Individual");

-- 12. Display for each customer the gap (in days) between consecutive orders.
select customer_id, order_date, lag(order_date) over(partition by customer_id order by order_date) as prev_date, 
datediff(order_date, lag(order_date) over(partition by customer_id order by order_date)) as gap_days
from orders
order by customer_id, order_date;

-- 13. Find which customer has the highest average order value per segment using a window function.
with seg as(
select c.customer_name, c.segment, avg(o.order_amount) over(partition by c.customer_name) as avg_order_value
from customers c
join orders o 
on c.customer_id = o.customer_id
),
segg as(
select *, rank() over(partition by segment order by avg_order_value desc) as rnk from seg
)
select distinct segment, customer_name, avg_order_value from segg
where rnk=1;

-- 14. Show running total of orders per month (based on order_date) for the entire company.
select month_name, total_orders, sum(total_orders) over(order by m_num) as running_total from (
select month(order_date) as m_num, date_format(order_date, '%M') as month_name, 
count(order_id) over(partition by month(order_date)) as total_orders,
row_number() over(partition by month(order_date) order by order_date) as rn
from orders
) cnt
where rn=1;

-- 15. Find percentile rank of each order amount among all orders.
select order_amount, percent_rank() over(order by order_amount) from orders;

-- 16. Identify customers who are consistently in the top 10% of spenders by order amount.
with cust_total as(
select distinct customer_id, sum(order_amount) over(partition by customer_id) as total_spent
from orders
),
ranked as(
select customer_id, total_spent, rank() over(order by total_spent desc) as rnk, count(*) over() * 0.10 as top_10
from cust_total
)
select customer_id, total_spent from ranked
where rnk>=top_10;

-- 17. Display for each city, the most recent 2 completed orders.
with something as (
select c.city, o.order_id, o.order_date, o.status, row_number() over(partition by c.city order by o.order_date desc) as rnk 
from customers c
join orders o 
on c.customer_id=o.customer_id
where o.status = "Completed"
)
select * from something
where rnk <=2;

-- 18. Compare each customer’s total spend with the overall company average.
with comp as(
select distinct c.customer_name, sum(o.order_amount) over(partition by c.customer_name) as total_amount
from customers c
join orders o
on c.customer_id=o.customer_id
)
select *, avg(total_amount) over() as overall_avg from comp;

-- 19. Show customer’s lifetime value (sum of orders) and how it ranks globally.
with lt as(
select distinct c.customer_name, sum(o.order_amount) over(partition by c.customer_name) as total_amount
from customers c
join orders o
on c.customer_id=o.customer_id
)
select *, rank() over(order by total_amount desc) as lifetime_value from lt;


select order_id, date_format(order_date, "%M") as datee from orders;
select order_id, monthname(order_date) as datee from orders;
select * from orders
where monthname(order_date)="June";

select customer_id, datediff(current_date(), '2024-06-20') as noofdays from orders; 

select * from employeedata;

select distinct year(order_date) as yr, count(customer_id) over(partition by year(order_date)) as cont from orders;

select employe_name, quarter(str_to_date(joining_date, "%d-%m-%y")) as q from employeedata;

select employe_name, round(year(current_date)-year(str_to_date(joining_date, "%d-%m-%y")), 2) as exp from employeedata;

select *, dayname(str_to_date(joining_date, "%d-%m-%Y")) from employeedata
where dayname(str_to_date(joining_date, "%d-%m-%Y")) in ("saturday", "sunday");

select year(str_to_date(joining_date, "%d-%m-%Y")) as yr, avg(salary) as avgs from employeedata
group by yr;

select employe_name, dept_name, min(str_to_date(joining_date, "%d-%m-%Y")) over(partition by dept_name) as Earliest_date, 
max(str_to_date(joining_date, "%d-%m-%Y")) over(partition by dept_name) as latest_date 
from employeedata;

select dept_name, monthname(str_to_date(joining_date, "%d-%m-%Y")) as month_name, 
count(employe_id) over(partition by month(str_to_date(joining_date, "%d-%m-%Y")), dept_name) as no_of_emp
from employeedata;

-- List employees who completed exactly 2 years in the company.
select employe_name, joining_date, timestampdiff(year, str_to_date(joining_date, "%d-%m-%Y"), current_date()) as exp from employeedata
where timestampdiff(year, str_to_date(joining_date, "%d-%m-%Y"), current_date()) = 2;

select employe_name, joining_date from employeedata
where month(str_to_date(joining_date, "%d-%m-%Y")) = month(current_date());

select employe_name, joining_date from employeedata
where year(date_add(str_to_date(joining_date, "%d-%m-%Y"), interval 5 year)) = year(current_date())+1;

select * from employeedata
where str_to_date(joining_date, "%d-%m-%Y") < str_to_date(concat(year(current_date()), "-04-01"), "%Y-%m-%d");

select * from employeedata;

select *, 
case
when timestampdiff(year,str_to_date(joining_date, "%d-%m-%Y"), current_date()) < 1 then "Fresher"
when timestampdiff(year,str_to_date(joining_date, "%d-%m-%Y"), current_date()) between 1 and 3 then "mid_level"
else "exp"
end as ts
from employeedata;

