from datetime import datetime
from logging import Logger
from uuid import UUID

from lib.kafka_connect import KafkaConsumer
from cdm_loader.repository.cdm_repository import CdmRepository


class CdmMessageProcessor:
    def __init__(
        self,
        consumer: KafkaConsumer,
        repository: CdmRepository,
        logger: Logger,
    ) -> None:
        self._consumer = consumer
        self._repository = repository
        self._logger = logger
        self._batch_size = 100

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()

            if not msg:
                break

            self._logger.info(f"Received message: {msg}")

            user_id = UUID(msg["user_id"])
            products = msg["products"]

            for product in products:
                self._repository.upsert_user_product_counter(
                    user_id=user_id,
                    product_id=UUID(product["product_id"]),
                    product_name=product["product_name"],
                    order_cnt=1,
                )

                self._repository.upsert_user_category_counter(
                    user_id=user_id,
                    category_id=UUID(product["category_id"]),
                    category_name=product["category_name"],
                    order_cnt=1,
                )

            self._consumer.commit()

        self._logger.info(f"{datetime.utcnow()}: FINISH")