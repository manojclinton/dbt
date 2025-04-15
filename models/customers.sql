select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders

    from {{ref("stg_customers")}} as customers

    left join {{ref("stg_customer_orders")}} as customer_orders using (customer_id)