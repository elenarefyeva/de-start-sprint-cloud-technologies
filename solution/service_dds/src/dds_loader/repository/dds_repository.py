import uuid
from datetime import datetime

from lib.pg import PgConnect


class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def h_order_insert(
        self,
        order_id: int,
        order_dt: datetime,
        load_dt: datetime,
        load_src: str
    ) -> uuid.UUID:

        h_order_pk = uuid.uuid5(
            uuid.NAMESPACE_DNS,
            f"order:{order_id}"
        )

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.h_order (
                        h_order_pk,
                        order_id,
                        order_dt,
                        load_dt,
                        load_src
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (h_order_pk) DO NOTHING;
                    """,
                    (
                        h_order_pk,
                        order_id,
                        order_dt,
                        load_dt,
                        load_src
                    )
                )

        return h_order_pk

    def h_user_insert(
        self,
        user_id: str,
        load_dt: datetime,
        load_src: str
    ) -> uuid.UUID:

        h_user_pk = uuid.uuid5(
            uuid.NAMESPACE_DNS,
            f"user:{user_id}"
        )

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.h_user (
                        h_user_pk,
                        user_id,
                        load_dt,
                        load_src
                    )
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (h_user_pk) DO NOTHING;
                    """,
                    (
                        h_user_pk,
                        user_id,
                        load_dt,
                        load_src
                    )
                )

        return h_user_pk

    def h_restaurant_insert(
        self,
        restaurant_id: str,
        load_dt: datetime,
        load_src: str
    ) -> uuid.UUID:

        h_restaurant_pk = uuid.uuid5(
            uuid.NAMESPACE_DNS,
            f"restaurant:{restaurant_id}"
        )

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.h_restaurant (
                        h_restaurant_pk,
                        restaurant_id,
                        load_dt,
                        load_src
                    )
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (h_restaurant_pk) DO NOTHING;
                    """,
                    (
                        h_restaurant_pk,
                        restaurant_id,
                        load_dt,
                        load_src
                    )
                )

        return h_restaurant_pk

    def h_product_insert(
        self,
        product_id: str,
        load_dt: datetime,
        load_src: str
    ) -> uuid.UUID:

        h_product_pk = uuid.uuid5(
            uuid.NAMESPACE_DNS,
            f"product:{product_id}"
        )

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.h_product (
                        h_product_pk,
                        product_id,
                        load_dt,
                        load_src
                    )
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (h_product_pk) DO NOTHING;
                    """,
                    (
                        h_product_pk,
                        product_id,
                        load_dt,
                        load_src
                    )
                )

        return h_product_pk

    def h_category_insert(
        self,
        category_name: str,
        load_dt: datetime,
        load_src: str
    ) -> uuid.UUID:

        h_category_pk = uuid.uuid5(
            uuid.NAMESPACE_DNS,
            f"category:{category_name}"
        )

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.h_category (
                        h_category_pk,
                        category_name,
                        load_dt,
                        load_src
                    )
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (h_category_pk) DO NOTHING;
                    """,
                    (
                        h_category_pk,
                        category_name,
                        load_dt,
                        load_src
                    )
                )

        return h_category_pk

    def s_order_cost_insert(
        self,
        h_order_pk: uuid.UUID,
        cost: int,
        payment: int,
        load_dt: datetime,
        load_src: str
    ) -> None:

        hk_order_cost_hashdiff = uuid.uuid5(
            uuid.NAMESPACE_DNS,
            f"order_cost:{cost}:{payment}"
        )

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.s_order_cost (
                        h_order_pk,
                        cost,
                        payment,
                        load_dt,
                        load_src,
                        hk_order_cost_hashdiff
                    )
                    SELECT %s, %s, %s, %s, %s, %s
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM dds.s_order_cost
                        WHERE h_order_pk = %s
                        AND hk_order_cost_hashdiff = %s
                    );
                    """,
                    (
                        h_order_pk,
                        cost,
                        payment,
                        load_dt,
                        load_src,
                        hk_order_cost_hashdiff,
                        h_order_pk,
                        hk_order_cost_hashdiff
                    )
                )

    def s_order_status_insert(
        self,
        h_order_pk: uuid.UUID,
        status: str,
        load_dt: datetime,
        load_src: str
    ) -> None:

        hk_order_status_hashdiff = uuid.uuid5(
            uuid.NAMESPACE_DNS,
            f"order_status:{status}"
        )

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.s_order_status (
                        h_order_pk,
                        status,
                        load_dt,
                        load_src,
                        hk_order_status_hashdiff
                    )
                    SELECT %s, %s, %s, %s, %s
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM dds.s_order_status
                        WHERE h_order_pk = %s
                        AND hk_order_status_hashdiff = %s
                    );
                    """,
                    (
                        h_order_pk,
                        status,
                        load_dt,
                        load_src,
                        hk_order_status_hashdiff,
                        h_order_pk,
                        hk_order_status_hashdiff
                    )
                )

    def s_product_names_insert(
        self,
        h_product_pk: uuid.UUID,
        name: str,
        load_dt: datetime,
        load_src: str
    ) -> None:

        hk_product_names_hashdiff = uuid.uuid5(
            uuid.NAMESPACE_DNS,
            f"product_name:{name}"
        )

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.s_product_names (
                        h_product_pk,
                        name,
                        load_dt,
                        load_src,
                        hk_product_names_hashdiff
                    )
                    SELECT %s, %s, %s, %s, %s
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM dds.s_product_names
                        WHERE h_product_pk = %s
                        AND hk_product_names_hashdiff = %s
                    );
                    """,
                    (
                        h_product_pk,
                        name,
                        load_dt,
                        load_src,
                        hk_product_names_hashdiff,
                        h_product_pk,
                        hk_product_names_hashdiff
                    )
                )

    def s_restaurant_names_insert(
        self,
        h_restaurant_pk: uuid.UUID,
        name: str,
        load_dt: datetime,
        load_src: str
    ) -> None:

        hk_restaurant_names_hashdiff = uuid.uuid5(
            uuid.NAMESPACE_DNS,
            f"restaurant_name:{name}"
        )

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.s_restaurant_names (
                        h_restaurant_pk,
                        name,
                        load_dt,
                        load_src,
                        hk_restaurant_names_hashdiff
                    )
                    SELECT %s, %s, %s, %s, %s
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM dds.s_restaurant_names
                        WHERE h_restaurant_pk = %s
                        AND hk_restaurant_names_hashdiff = %s
                    );
                    """,
                    (
                        h_restaurant_pk,
                        name,
                        load_dt,
                        load_src,
                        hk_restaurant_names_hashdiff,
                        h_restaurant_pk,
                        hk_restaurant_names_hashdiff
                    )
                )

    def s_user_names_insert(
        self,
        h_user_pk: uuid.UUID,
        username: str,
        userlogin: str,
        load_dt: datetime,
        load_src: str
    ) -> None:

        hk_user_names_hashdiff = uuid.uuid5(
            uuid.NAMESPACE_DNS,
            f"user_names:{username}:{userlogin}"
        )

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.s_user_names (
                        h_user_pk,
                        username,
                        userlogin,
                        load_dt,
                        load_src,
                        hk_user_names_hashdiff
                    )
                    SELECT %s, %s, %s, %s, %s, %s
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM dds.s_user_names
                        WHERE h_user_pk = %s
                        AND hk_user_names_hashdiff = %s
                    );
                    """,
                    (
                        h_user_pk,
                        username,
                        userlogin,
                        load_dt,
                        load_src,
                        hk_user_names_hashdiff,
                        h_user_pk,
                        hk_user_names_hashdiff
                    )
                )

    def l_order_user_insert(
        self,
        h_order_pk: uuid.UUID,
        h_user_pk: uuid.UUID,
        load_dt: datetime,
        load_src: str
    ) -> uuid.UUID:

        hk_order_user_pk = uuid.uuid5(
            uuid.NAMESPACE_DNS,
            f"order_user:{h_order_pk}:{h_user_pk}"
        )

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.l_order_user (
                        hk_order_user_pk,
                        h_order_pk,
                        h_user_pk,
                        load_dt,
                        load_src
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (hk_order_user_pk) DO NOTHING;
                    """,
                    (
                        hk_order_user_pk,
                        h_order_pk,
                        h_user_pk,
                        load_dt,
                        load_src
                    )
                )

        return hk_order_user_pk

    def l_order_product_insert(
        self,
        h_order_pk: uuid.UUID,
        h_product_pk: uuid.UUID,
        load_dt: datetime,
        load_src: str
    ) -> uuid.UUID:

        hk_order_product_pk = uuid.uuid5(
            uuid.NAMESPACE_DNS,
            f"order_product:{h_order_pk}:{h_product_pk}"
        )

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.l_order_product (
                        hk_order_product_pk,
                        h_order_pk,
                        h_product_pk,
                        load_dt,
                        load_src
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (hk_order_product_pk) DO NOTHING;
                    """,
                    (
                        hk_order_product_pk,
                        h_order_pk,
                        h_product_pk,
                        load_dt,
                        load_src
                    )
                )

        return hk_order_product_pk

    def l_product_category_insert(
        self,
        h_product_pk: uuid.UUID,
        h_category_pk: uuid.UUID,
        load_dt: datetime,
        load_src: str
    ) -> uuid.UUID:

        hk_product_category_pk = uuid.uuid5(
            uuid.NAMESPACE_DNS,
            f"product_category:{h_product_pk}:{h_category_pk}"
        )

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.l_product_category (
                        hk_product_category_pk,
                        h_product_pk,
                        h_category_pk,
                        load_dt,
                        load_src
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (hk_product_category_pk) DO NOTHING;
                    """,
                    (
                        hk_product_category_pk,
                        h_product_pk,
                        h_category_pk,
                        load_dt,
                        load_src
                    )
                )

        return hk_product_category_pk

    def l_product_restaurant_insert(
        self,
        h_product_pk: uuid.UUID,
        h_restaurant_pk: uuid.UUID,
        load_dt: datetime,
        load_src: str
    ) -> uuid.UUID:

        hk_product_restaurant_pk = uuid.uuid5(
            uuid.NAMESPACE_DNS,
            f"product_restaurant:{h_product_pk}:{h_restaurant_pk}"
        )

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.l_product_restaurant (
                        hk_product_restaurant_pk,
                        h_product_pk,
                        h_restaurant_pk,
                        load_dt,
                        load_src
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (hk_product_restaurant_pk) DO NOTHING;
                    """,
                    (
                        hk_product_restaurant_pk,
                        h_product_pk,
                        h_restaurant_pk,
                        load_dt,
                        load_src
                    )
                )

        return hk_product_restaurant_pk
