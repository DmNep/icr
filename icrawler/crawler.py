"""Класс базового класса"""

import logging
import sys
import time
from importlib import import_module

from . import storage as storage_package
from .downloader import Downloader
from .feeder import Feeder
from .parser import Parser
from .storage import BaseStorage
from .utils import ProxyPool, Session, Signal


class Crawler:
    """Базовый класс для сканеров

    Атрибуты:
        session (Session):Объект сеанса.
        feeder (Feeder):Объект подачи.
        parser (Parser):Объект анализатора.
        downloader (Downloader):Объект загрузчика.
        signal (Signal):Объект сигнала, разделяемый всеми компонентами,
                         используется для связи между потоками
        logger (Logger):Объект регистрации, используемый для регистрации
    """

    def __init__(
        self,
        feeder_cls=Feeder,
        parser_cls=Parser,
        downloader_cls=Downloader,
        feeder_threads=1,
        parser_threads=1,
        downloader_threads=1,
        storage={"backend": "FileSystem", "root_dir": "images"},
        log_level=logging.INFO,
        extra_feeder_args=None,
        extra_parser_args=None,
        extra_downloader_args=None,
    ):
        """Компоненты init с именами классов и другими аргументами.

        Args:
            feeder_cls: Класс подачи
            parser_cls: класс анализатора
            downloader_cls: Класс загрузчика.
            feeder_threads: номер потока, используемый в подаче
            parser_threads: номер потока, используемый парсером
            downloader_threads:номер потока, используемый загрузчиком
            storage (dict or BaseStorage): Конфигурация бэкэнд хранения
            log_level:Уровень регистрации для регистрации
        """

        self.set_logger(log_level)
        self.set_proxy_pool()
        self.set_session()
        self.init_signal()
        self.set_storage(storage)
        # set feeder, parser and downloader
        feeder_kwargs = {} if extra_feeder_args is None else extra_feeder_args
        parser_kwargs = {} if extra_parser_args is None else extra_parser_args
        downloader_kwargs = {} if extra_downloader_args is None else extra_downloader_args
        self.feeder = feeder_cls(feeder_threads, self.signal, self.session, **feeder_kwargs)
        self.parser = parser_cls(parser_threads, self.signal, self.session, **parser_kwargs)
        self.downloader = downloader_cls(
            downloader_threads, self.signal, self.session, self.storage, **downloader_kwargs
        )
        # connect all components
        self.feeder.connect(self.parser).connect(self.downloader)

    def set_logger(self, log_level=logging.INFO):
        """Настройте журнал с помощью log_level."""
        logging.basicConfig(
            format="%(asctime)s - %(levelname)s - %(name)s - %(message)s", level=log_level, stream=sys.stderr
        )
        self.logger = logging.getLogger(__name__)
        logging.getLogger("requests").setLevel(logging.WARNING)

    def init_signal(self):
        """Инициатор сигнал

        Добавлены 3 сигнала: ``feeder_exited``, ``parser_exited`` и
        ``reach_max_num``.
        """
        self.signal = Signal()
        self.signal.set(feeder_exited=False, parser_exited=False, reach_max_num=False)

    def set_storage(self, storage):
        """Установите бэкэнд хранения для загрузчика

       Для получения полного списка поддерживаемого бэкэнда хранения, пожалуйста, смотрите:mod:`storage`.

        Args:
            storage (dict or BaseStorage): Конфигурация бэкэнд или экземпляра хранения

        """
        if isinstance(storage, BaseStorage):
            self.storage = storage
        elif isinstance(storage, dict):
            if "backend" not in storage and "root_dir" in storage:
                storage["backend"] = "FileSystem"
            try:
                backend_cls = getattr(storage_package, storage["backend"])
            except AttributeError:
                try:
                    backend_cls = import_module(storage["backend"])
                except ImportError:
                    self.logger.error("Не могу найти модуль бэкэнда %s", storage["backend"])
                    sys.exit()
            kwargs = storage.copy()
            del kwargs["backend"]
            self.storage = backend_cls(**kwargs)
        else:
            raise TypeError('"хранилище "должно быть объектом хранения или дикта')

    def set_proxy_pool(self, pool=None):
        """Построить прокси -бассейн

       По умолчанию прокси -сервер не используется.
        Args:
            pool (ProxyPool, optional): a :obj:`ProxyPool` объект
        """
        self.proxy_pool = ProxyPool() if pool is None else pool

    def set_session(self, headers=None):
        """IСеанс NIT с заголовками по умолчанию или пользовательским

        Args:
            headers: DICT из заголовков (по умолчанию нет, таким образом, используя по умолчанию
                     Заголовок TO INIT The Session)
        """
        if headers is None:
            headers = {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
                    " AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/88.0.4324.104 Safari/537.36"
                )
            }
        elif not isinstance(headers, dict):
            raise TypeError('"Заголовки "должны быть объектом DICT')

        self.session = Session(self.proxy_pool)
        self.session.headers.update(headers)

    def crawl(self, feeder_kwargs=None, parser_kwargs=None, downloader_kwargs=None):
        """Начать ползать

        Этот метод запустит фидер, анализатор и загрузку и ждать
        пока все нити не выйдут.

        Args:
            feeder_kwargs (dict, optional): Аргументы, которые должны быть переданы в `fired.start ()` ``
            parser_kwargs (dict, optional): Аргументы, которые должны быть переданы `` parser.start () `` `
            downloader_kwargs (dict, optional): Аргументы, которые должны быть переданы
                ``downloader.start()``
        """
        self.signal.reset()
        self.logger.info("start crawling...")

        feeder_kwargs = {} if feeder_kwargs is None else feeder_kwargs
        parser_kwargs = {} if parser_kwargs is None else parser_kwargs
        downloader_kwargs = {} if downloader_kwargs is None else downloader_kwargs

        self.logger.info("starting %d Фидер...", self.feeder.thread_num)
        self.feeder.start(**feeder_kwargs)

        self.logger.info("starting %d Диаризовательские нити...", self.parser.thread_num)
        self.parser.start(**parser_kwargs)

        self.logger.info("starting %d потоки загрузчиков...", self.downloader.thread_num)
        self.downloader.start(**downloader_kwargs)

        while True:
            if not self.feeder.is_alive():
                self.signal.set(feeder_exited=True)
            if not self.parser.is_alive():
                self.signal.set(parser_exited=True)
            if not self.downloader.is_alive():
                break
            time.sleep(1)

        if not self.feeder.in_queue.empty():
            self.feeder.clear_buffer()
        if not self.parser.in_queue.empty():
            self.parser.clear_buffer()
        if not self.downloader.in_queue.empty():
            self.downloader.clear_buffer(True)

        self.logger.info("Задача ползания выполнена!")
