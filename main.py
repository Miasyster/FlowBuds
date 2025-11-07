"""
基于消息总线的生产者-消费者模式
使用消息总线(Message Bus)实现发布-订阅模式
"""
import threading
import queue
import time
import random
from typing import Dict, List, Callable
from enum import Enum


class MessageType(Enum):
    """消息类型枚举"""
    ORDER = "订单"
    PAYMENT = "支付"
    INVENTORY = "库存"
    SHIPPING = "物流"
    NOTIFICATION = "通知"


class Message:
    """消息对象"""

    def __init__(self, msg_type: MessageType, content: str, sender: str):
        self.msg_type = msg_type
        self.content = content
        self.sender = sender
        self.timestamp = time.time()

    def __str__(self):
        return f"[{self.msg_type.value}] {self.sender}: {self.content}"


class MessageBus:
    """
    消息总线 - 核心组件
    负责消息的路由、分发和管理
    """

    def __init__(self, name="主消息总线"):
        self.name = name
        # 为每种消息类型维护一个订阅者队列
        self.subscribers: Dict[MessageType, List[queue.Queue]] = {}
        self.lock = threading.Lock()
        self.message_count = 0

        # 初始化所有消息类型的订阅者列表
        for msg_type in MessageType:
            self.subscribers[msg_type] = []

    def subscribe(self, msg_type: MessageType, subscriber_queue: queue.Queue):
        """订阅某类消息"""
        with self.lock:
            if subscriber_queue not in self.subscribers[msg_type]:
                self.subscribers[msg_type].append(subscriber_queue)
                print(f"📡 [{self.name}] 新订阅者注册: {msg_type.value}")

    def unsubscribe(self, msg_type: MessageType, subscriber_queue: queue.Queue):
        """取消订阅"""
        with self.lock:
            if subscriber_queue in self.subscribers[msg_type]:
                self.subscribers[msg_type].remove(subscriber_queue)

    def publish(self, message: Message):
        """发布消息到总线"""
        with self.lock:
            self.message_count += 1
            subscribers = self.subscribers.get(message.msg_type, [])

            print(f"📤 [{self.name}] 发布消息 #{self.message_count}: {message}")
            print(f"   → 将分发给 {len(subscribers)} 个订阅者")

            # 将消息分发给所有订阅者
            for sub_queue in subscribers:
                sub_queue.put(message)

    def get_stats(self):
        """获取总线统计信息"""
        with self.lock:
            stats = {}
            for msg_type, subs in self.subscribers.items():
                stats[msg_type.value] = len(subs)
            return stats


class Producer(threading.Thread):
    """生产者 - 向消息总线发布消息"""

    def __init__(self, name: str, bus: MessageBus, msg_type: MessageType, count=5):
        super().__init__()
        self.name = name
        self.bus = bus
        self.msg_type = msg_type
        self.count = count

    def run(self):
        """生产并发布消息"""
        print(f"🏭 [生产者-{self.name}] 启动，准备生产 {self.count} 条 {self.msg_type.value} 消息")

        for i in range(self.count):
            # 模拟生产耗时
            time.sleep(random.uniform(0.2, 0.8))

            # 创建消息
            content = f"{self.msg_type.value}数据-{i + 1}"
            message = Message(self.msg_type, content, self.name)

            # 发布到总线
            self.bus.publish(message)

        print(f"✅ [生产者-{self.name}] 完成生产")


class Consumer(threading.Thread):
    """消费者 - 从消息总线订阅并消费消息"""

    def __init__(self, name: str, bus: MessageBus, interested_types: List[MessageType], count=5):
        super().__init__()
        self.name = name
        self.bus = bus
        self.interested_types = interested_types
        self.count = count
        self.message_queue = queue.Queue(maxsize=20)
        self.daemon = True  # 设置为守护线程

        # 订阅感兴趣的消息类型
        for msg_type in interested_types:
            bus.subscribe(msg_type, self.message_queue)

    def run(self):
        """消费消息"""
        types_str = ", ".join([t.value for t in self.interested_types])
        print(f"🛒 [消费者-{self.name}] 启动，订阅: {types_str}")

        consumed = 0
        while consumed < self.count:
            try:
                # 从队列获取消息
                message = self.message_queue.get(timeout=2)

                # 模拟处理耗时
                time.sleep(random.uniform(0.3, 1.0))

                print(f"   ✓ [消费者-{self.name}] 处理: {message}")
                consumed += 1

            except queue.Empty:
                print(f"   ⏳ [消费者-{self.name}] 等待消息...")

        print(f"✅ [消费者-{self.name}] 完成消费")


def demo_simple():
    """示例1: 简单的单类型消息"""
    print("\n" + "=" * 80)
    print("示例1: 简单场景 - 订单消息处理")
    print("=" * 80 + "\n")

    # 创建消息总线
    bus = MessageBus("订单总线")

    # 创建生产者
    producers = [
        Producer("订单服务A", bus, MessageType.ORDER, count=3),
        Producer("订单服务B", bus, MessageType.ORDER, count=3),
    ]

    # 创建消费者
    consumers = [
        Consumer("订单处理器1", bus, [MessageType.ORDER], count=3),
        Consumer("订单处理器2", bus, [MessageType.ORDER], count=3),
    ]

    # 启动所有线程
    for p in producers:
        p.start()
    for c in consumers:
        c.start()

    # 等待生产者完成
    for p in producers:
        p.join()

    # 等待消费者完成
    for c in consumers:
        c.join(timeout=5)

    print(f"\n📊 总线统计: {bus.get_stats()}")


def demo_complex():
    """示例2: 复杂的多类型消息场景"""
    print("\n" + "=" * 80)
    print("示例2: 复杂场景 - 电商系统消息流转")
    print("=" * 80 + "\n")

    # 创建消息总线
    bus = MessageBus("电商消息总线")

    # 创建多类型生产者
    producers = [
        Producer("订单系统", bus, MessageType.ORDER, count=4),
        Producer("支付系统", bus, MessageType.PAYMENT, count=4),
        Producer("仓储系统", bus, MessageType.INVENTORY, count=3),
        Producer("物流系统", bus, MessageType.SHIPPING, count=3),
    ]

    # 创建专门的消费者（每个消费者关注不同的消息类型）
    consumers = [
        # 订单处理器 - 关注订单和支付
        Consumer("订单处理器", bus, [MessageType.ORDER, MessageType.PAYMENT], count=4),

        # 库存管理器 - 关注订单和库存
        Consumer("库存管理器", bus, [MessageType.ORDER, MessageType.INVENTORY], count=4),

        # 物流协调器 - 关注支付和物流
        Consumer("物流协调器", bus, [MessageType.PAYMENT, MessageType.SHIPPING], count=4),

        # 通知服务 - 关注所有消息
        Consumer("通知服务", bus, [MessageType.ORDER, MessageType.PAYMENT,
                                   MessageType.INVENTORY, MessageType.SHIPPING], count=8),
    ]

    # 启动所有线程
    for p in producers:
        p.start()

    time.sleep(0.5)  # 稍微延迟启动消费者

    for c in consumers:
        c.start()

    # 等待生产者完成
    for p in producers:
        p.join()

    # 等待消费者完成
    for c in consumers:
        c.join(timeout=10)

    print(f"\n📊 总线统计: {bus.get_stats()}")
    print(f"📈 总共处理消息数: {bus.message_count}")


def demo_pipeline():
    """示例3: 流水线式处理"""
    print("\n" + "=" * 80)
    print("示例3: 流水线场景 - 消息链式处理")
    print("=" * 80 + "\n")

    bus = MessageBus("流水线总线")

    class ProcessingConsumer(threading.Thread):
        """处理后转发的消费者"""

        def __init__(self, name: str, bus: MessageBus,
                     input_type: MessageType, output_type: MessageType, count=3):
            super().__init__()
            self.name = name
            self.bus = bus
            self.input_type = input_type
            self.output_type = output_type
            self.count = count
            self.message_queue = queue.Queue()
            bus.subscribe(input_type, self.message_queue)

        def run(self):
            print(f"⚙️  [处理器-{self.name}] 启动: {self.input_type.value} → {self.output_type.value}")

            processed = 0
            while processed < self.count:
                try:
                    message = self.message_queue.get(timeout=3)
                    time.sleep(random.uniform(0.2, 0.5))

                    print(f"   ⚙️  [处理器-{self.name}] 处理: {message.content}")

                    # 处理完后发布新消息
                    new_content = f"已处理-{message.content}"
                    new_message = Message(self.output_type, new_content, self.name)
                    self.bus.publish(new_message)

                    processed += 1
                except queue.Empty:
                    break

            print(f"✅ [处理器-{self.name}] 完成")

    # 创建流水线: ORDER → PAYMENT → SHIPPING
    producer = Producer("初始订单", bus, MessageType.ORDER, count=3)

    processor1 = ProcessingConsumer("支付处理", bus, MessageType.ORDER, MessageType.PAYMENT, count=3)
    processor2 = ProcessingConsumer("发货处理", bus, MessageType.PAYMENT, MessageType.SHIPPING, count=3)

    final_consumer = Consumer("最终处理", bus, [MessageType.SHIPPING], count=3)

    # 启动
    processor1.start()
    processor2.start()
    final_consumer.start()

    time.sleep(0.5)
    producer.start()

    producer.join()
    processor1.join()
    processor2.join()
    final_consumer.join(timeout=5)

    print(f"\n📊 总线统计: {bus.get_stats()}")


if __name__ == "__main__":
    demo_simple()
    time.sleep(1)

    demo_complex()
    time.sleep(1)

    demo_pipeline()

    print("\n" + "=" * 80)
    print("所有示例运行完成！")
    print("=" * 80)