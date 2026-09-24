# Cloud Technologies Project

Проект реализует обработку заказов в потоковом режиме с использованием Kafka и микросервисной архитектуры.

Данные проходят через три сервиса:

`STG Service → Kafka → DDS Service → Kafka → CDM Service → PostgreSQL`

## Services

### DDS Service

Читает сообщения из Kafka, обрабатывает данные заказов и сохраняет их в DDS-слой PostgreSQL.

После обработки формирует сообщения для CDM-сервиса и публикует их в Kafka-топик `dds-service-orders`.

### CDM Service

Читает сообщения из Kafka-топика `dds-service-orders` и обновляет пользовательские витрины в PostgreSQL:

- `cdm.user_product_counters` — количество заказов товаров пользователями;
- `cdm.user_category_counters` — количество заказов по категориям товаров пользователями.

## Deployment

DDS и CDM сервисы контейнеризированы с помощью Docker и развёрнуты в Kubernetes с использованием Helm.

Конфигурация Helm-чартов находится в:

- `service_dds/app`
- `service_cdm/app`

Параметры подключения передаются сервисам через ConfigMap.

Локальные значения с паролями не хранятся в репозитории.

## Container Registry

Docker-образы сервисов:

- DDS: `cr.yandex/crpb9d80i8ficf1tlgsj/dds_service:v2026-09-24`
- CDM: `cr.yandex/crpb9d80i8ficf1tlgsj/cdm_service:v2026-09-24`