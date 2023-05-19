import os
import pymysql as sql

def connect():
    connection = sql.connect(
        user=os.environ["USER"],
        password=os.environ["PASSWORD"],
        host=os.environ["HOST"],
        database=os.environ["DATABASE"],
        port=3306,
    )
    return connection


def lambda_handler(event, context):
    print(event)
    with connect() as connection:
        with connection.cursor() as cursor:
            try:
                cursor.execute(
                    """update customers c
                                set c.reward_points = (
                                    select points.rp
                                    from (
                                        with order_payment_data as (
                                            select
                                            o.oid,
                                            o.cid,
                                            p.rp
                                            from (
                                                select
                                                order_id as oid,
                                                customer_id as cid
                                                from orders
                                                where
                                                timestampdiff(
                                                    day,
                                                    order_delivered_customer_date,
                                                    current_timestamp()
                                                ) = 10
                                            ) o
                                            join (
                                                select
                                                order_id as oid,
                                                reward_points as rp
                                                from
                                                payments
                                            ) p on o.oid = p.oid
                                        ),
                                        customer_reward_points as (
                                            select
                                            cid,
                                            sum(rp) as rp
                                            from order_payment_data
                                            group by cid
                                        )
                                        select
                                        crp.cid,
                                        crp.rp + c.reward_points as rp
                                        from customer_reward_points crp
                                        join customers c on crp.cid = c.customer_id
                                    ) points
                                    where points.cid = c.customer_id
                                );""".upper()
                )
                connection.commit()
            except Exception as e:
                print(e)
                connection.rollback()