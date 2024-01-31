import logging
import queue
import time
from threading import current_thread
from urllib.parse import urlsplit

from .utils import ThreadPool


class Parser(ThreadPool):
    """Базовый класс для анализатора.

    Пул потоков парсеров, отвечающий за страницы загрузки и анализа,
    Извлечение URL -адресов файлов и поместите их в очередь ввода загрузчика.

    Атрибуты:
        global_signal:Сигнальный объект для передачи передачи модулей.
        session:Объект запросов.
        logger: Hearging.logger объект, используемый для ведения журнала.
        threads: Список, хранящий все потоки.
        thread_num: Целое число, указывающее количество потоков.
        lock: Объект потока.lock.
    """

    def __init__(self, thread_num, signal, session):
        """Инициативный анализатор с некоторыми общими переменными."""
        super().__init__(thread_num, name="parser")
        self.signal = signal
        self.session = session

    def parse(self, response, **kwargs):
        """Распокачивайте страницу и извлеките URL -адреса изображения, затем поместите ее в task_queue.

        Этот метод должен быть переопределен пользователями.

        :Example:

        >>> task = {}
        >>> self.output(task)  # Доктор: +пропустить
        """
        raise NotImplementedError

    def worker_exec(self, queue_timeout=2, req_timeout=5, max_retry=3, **kwargs):
        """Целевой метод работников.

        Сначала загрузите страницу, а затем позвоните в метод: func: `parse`.
        Поток анализатора выйдет в любой из следующих случаев:

        1. Все подачи фидеров вышли, а `` url_queue`` - пуст.
        2. Загруженный номер изображения достиг необходимого номера.

        Args:
            queue_timeout (int):Тайм -аут получения URL -адресов от `` url_queue``.
            req_timeout (int):Тайм -аут для получения запросов на загрузку страниц.
            max_retry (int):Максимальное время повторения, если запрос не удастся.
            **kwargs: Аргументы, которые должны быть переданы методу: func: `parse`.
        """
        while True:
            if self.signal.get("reach_max_num"):
                self.logger.info(
                    "Загруженное изображение достигли максимального номера, thread %s " "is ready to exit", current_thread().name
                )
                break
            # get the page url
            try:
                url = self.in_queue.get(timeout=queue_timeout)
            except queue.Empty:
                if self.signal.get("feeder_exited"):
                    self.logger.info("Больше нет страниц URL -адреса для потока %s посылка", current_thread().name)
                    break
                else:
                    self.logger.info("%s ждет новых URL -адресов страницы", current_thread().name)
                    continue
            except:
                self.logger.error("Исключение в потоке %s", current_thread().name)
                continue
            else:
                self.logger.debug(f"Начните получение страницы {url}")
            # fetch and parse the page
            retry = max_retry
            while retry > 0:
                try:
                    base_url = "{0.scheme}://{0.netloc}".format(urlsplit(url))
                    response = self.session.get(url, timeout=req_timeout, headers={"Referer": base_url})
                except Exception as e:
                    self.logger.error(
                        "Исключение поймано при получении страницы %s, " "ошибка: %s, оставшиеся время повторения: %d",
                        url,
                        e,
                        retry - 1,
                    )
                else:
                    self.logger.info(f"Страница результатов анализа {url}")
                    for task in self.parse(response, **kwargs):
                        while not self.signal.get("reach_max_num"):
                            try:
                                if isinstance(task, dict):
                                    self.output(task, timeout=1)
                                elif isinstance(task, str):
                                    # Этот случай работает только для GreedyCrawler,
                                    # которые должны вернуть URL обратно в
                                    # url_queue, грязная реализация
                                    self.input(task, timeout=1)
                            except queue.Full:
                                time.sleep(1)
                            except Exception as e:
                                self.logger.error(
                                    "Исключение поймано, когда задание положила %s в " "очередь, ошибка: %s", task, url
                                )
                            else:
                                break
                        if self.signal.get("reach_max_num"):
                            break
                    self.in_queue.task_done()
                    break
                finally:
                    retry -= 1
        self.logger.info(f"нить {current_thread().name} Выход")

    def __exit__(self):
        logging.info("Все выхода паризонов выходят")
