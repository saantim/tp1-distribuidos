import threading
import time

import pytest

from shared.middleware.rabbit_mq import (
    MessageMiddlewareExchangeRMQ,
    MessageMiddlewareQueueMQ,
)


@pytest.mark.usefixtures("rabbitmq_service")
class TestRabbitMQMiddlewareSimple:
    def test_queue_work_1_to_1(self, rabbitmq_host):
        queue_name = "test_queue_1_to_1"

        sender = MessageMiddlewareQueueMQ(rabbitmq_host, queue_name)
        receiver = MessageMiddlewareQueueMQ(rabbitmq_host, queue_name)

        result: list[bytes] = []

        def on_msg(ch, method, props, body):
            result.append(body)
            receiver.stop_consuming()

        try:
            sender.send(b"testing")
            receiver.start_consuming(on_msg)
        finally:
            try:
                sender.delete()
            except Exception:
                pass
            receiver.close()
            sender.close()

        assert result == [b"testing"]

    def test_queue_work_1_to_n(self, rabbitmq_host):
        queue_name = "test_queue_1_to_n"
        _ = [b"testing" for _ in range(500)]

        sender = MessageMiddlewareQueueMQ(rabbitmq_host, queue_name)
        c1 = MessageMiddlewareQueueMQ(rabbitmq_host, queue_name)
        c2 = MessageMiddlewareQueueMQ(rabbitmq_host, queue_name)

        result_1 = []
        result_2 = []
        wk_1_got_message = threading.Event()
        wk_2_got_message = threading.Event()

        def on1(ch, method, props, body):
            result_1.append(body)
            c1.stop_consuming()
            wk_1_got_message.set()

        def on2(ch, method, props, body):
            result_2.append(body)
            c2.stop_consuming()
            wk_2_got_message.set()

        t1 = threading.Thread(target=c1.start_consuming, args=(on1,))
        t1.daemon = True
        t1.start()

        sender.send("first_message")
        wk_1_got_message.wait(timeout=5)

        t2 = threading.Thread(target=c2.start_consuming, args=(on2,))
        t2.daemon = True
        t2.start()

        sender.send("second_message")
        wk_2_got_message.wait(timeout=5)

        assert result_1 == [b"first_message"]
        assert result_2 == [b"second_message"]

    def test_exchange_1_to_1(self, rabbitmq_host):
        exchange = "test_exchange_1_to_1"
        rk = "rk1"

        receiver = MessageMiddlewareExchangeRMQ(rabbitmq_host, exchange, [rk])
        sender = MessageMiddlewareExchangeRMQ(rabbitmq_host, exchange, [rk])

        result: list[bytes] = []
        done = threading.Event()

        def on_msg(ch, method, props, body):
            result.append(body)
            done.set()
            receiver.stop_consuming()

        try:
            t = threading.Thread(target=receiver.start_consuming, args=(on_msg,), daemon=True)
            t.start()
            time.sleep(0.3)

            sender.send(b"testing")

            assert done.wait(5), "No llegó mensaje por exchange"
            t.join(timeout=2)
        finally:
            try:
                sender.delete()
            except Exception:
                pass
            receiver.close()
            sender.close()

        assert result == [b"testing"]

    def test_exchange_1_to_n(self, rabbitmq_host):
        exchange = "test_exchange_1_to_n"
        rk = "rk1"

        r1 = MessageMiddlewareExchangeRMQ(rabbitmq_host, exchange, [rk])
        r2 = MessageMiddlewareExchangeRMQ(rabbitmq_host, exchange, [rk])
        sender = MessageMiddlewareExchangeRMQ(rabbitmq_host, exchange, [rk])

        result_1: list[bytes] = []
        result_2: list[bytes] = []
        e1, e2 = threading.Event(), threading.Event()

        def on1(ch, method, props, body):
            result_1.append(body)
            e1.set()
            r1.stop_consuming()

        def on2(ch, method, props, body):
            result_2.append(body)
            e2.set()
            r2.stop_consuming()

        try:
            t1 = threading.Thread(target=r1.start_consuming, args=(on1,), daemon=True)
            t2 = threading.Thread(target=r2.start_consuming, args=(on2,), daemon=True)
            t1.start()
            t2.start()
            time.sleep(0.3)

            sender.send(b"testing")

            assert e1.wait(5) and e2.wait(5), "Ambos consumidores deben recibir"
            t1.join(timeout=2)
            t2.join(timeout=2)
        finally:
            try:
                sender.delete()
            except Exception:
                pass
            r1.close()
            r2.close()
            sender.close()

        assert result_1 == [b"testing"] and result_2 == [b"testing"]
