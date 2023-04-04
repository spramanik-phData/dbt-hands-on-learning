{%- set payment_methods = ['credit_card', 'coupon', 'bank_transfer', 'gift_card'] -%}

with payments as (
    select * from {{ ref('stg_payments') }}
),

pivoted as (
    select 
        orderid,

        {% for payment_method in payment_methods %}
            sum(
                case when payment_method = '{{ payment_method }}' 1 else 0 end
            ) as {{ payment_method }}_amount{{',' if not loop.last }}
        {% endfor %}

    from payments
)

select * from pivoted