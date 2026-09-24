from datetime import datetime
from logging import Logger

from dds_loader.repository.dds_repository import DdsRepository
from lib.kafka_connect.kafka_connectors import KafkaConsumer, KafkaProducer


class DdsMessageProcessor:
    def __init__(
        self,
        consumer: KafkaConsumer,
        producer: KafkaProducer,
        repository: DdsRepository,
        logger: Logger
    ) -> None:

        self._consumer = consumer
        self._producer = producer
        self._repository = repository
        self._logger = logger

        self._batch_size = 30

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()

            if msg is None:
                break

            payload = msg["payload"]

            load_dt = datetime.utcnow()
            load_src = "kafka"

            order_id = payload["id"]
            order_dt = datetime.fromisoformat(payload["date"])

            user = payload["user"]
            restaurant = payload["restaurant"]
            products = payload["products"]

            cost = payload["cost"]
            payment = payload["payment"]
            status = payload["status"]

            # Загружаем основные Hub'ы

            h_order_pk = self._repository.h_order_insert(
                order_id=order_id,
                order_dt=order_dt,
                load_dt=load_dt,
                load_src=load_src
            )

            h_user_pk = self._repository.h_user_insert(
                user_id=user["id"],
                load_dt=load_dt,
                load_src=load_src
            )

            h_restaurant_pk = self._repository.h_restaurant_insert(
                restaurant_id=restaurant["id"],
                load_dt=load_dt,
                load_src=load_src
            )

            # Загружаем Satellite'ы заказа

            self._repository.s_order_cost_insert(
                h_order_pk=h_order_pk,
                cost=cost,
                payment=payment,
                load_dt=load_dt,
                load_src=load_src
            )

            self._repository.s_order_status_insert(
                h_order_pk=h_order_pk,
                status=status,
                load_dt=load_dt,
                load_src=load_src
            )

            # Связываем заказ с пользователем

            self._repository.l_order_user_insert(
                h_order_pk=h_order_pk,
                h_user_pk=h_user_pk,
                load_dt=load_dt,
                load_src=load_src
            )

            dds_products = []

            # Загружаем продукты

            for product in products:
                h_product_pk = self._repository.h_product_insert(
                    product_id=product["id"],
                    load_dt=load_dt,
                    load_src=load_src
                )

                h_category_pk = self._repository.h_category_insert(
                    category_name=product["category"],
                    load_dt=load_dt,
                    load_src=load_src
                )

                dds_products.append({
                    "product_id": str(h_product_pk),
                    "product_name": product["name"],
                    "category_id": str(h_category_pk),
                    "category_name": product["category"]
                })

                # Загружаем название продукта

                self._repository.s_product_names_insert(
                    h_product_pk=h_product_pk,
                    name=product["name"],
                    load_dt=load_dt,
                    load_src=load_src
                )

                # Связываем продукт с заказом

                self._repository.l_order_product_insert(
                    h_order_pk=h_order_pk,
                    h_product_pk=h_product_pk,
                    load_dt=load_dt,
                    load_src=load_src
                )

                # Связываем продукт с категорией

                self._repository.l_product_category_insert(
                    h_product_pk=h_product_pk,
                    h_category_pk=h_category_pk,
                    load_dt=load_dt,
                    load_src=load_src
                )

                # Связываем продукт с рестораном

                self._repository.l_product_restaurant_insert(
                    h_product_pk=h_product_pk,
                    h_restaurant_pk=h_restaurant_pk,
                    load_dt=load_dt,
                    load_src=load_src
                )

            dds_message = {
                "user_id": str(h_user_pk),
                "products": dds_products
            }

            # Загружаем данные пользователя

            self._repository.s_user_names_insert(
                h_user_pk=h_user_pk,
                username=user["name"],
                userlogin=user["login"],
                load_dt=load_dt,
                load_src=load_src
            )

            # Загружаем название ресторана

            self._repository.s_restaurant_names_insert(
                h_restaurant_pk=h_restaurant_pk,
                name=restaurant["name"],
                load_dt=load_dt,
                load_src=load_src
            )

            self._producer.produce(dds_message)

            self._consumer.commit()

            self._logger.info(f"Received message: {msg}")

        self._logger.info(f"{datetime.utcnow()}: FINISH")
