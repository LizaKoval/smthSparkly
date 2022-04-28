import time

class Timer:

    def __init__(self):
        self._start_time = None # _ prefix is used to hide this attribute from user of Timer class

    def start(self):
        """Запуск нового таймера"""

        if self._start_time is not None:
            print("Таймер уже работает. Используйте .stop() чтобы его остановить")

        self._start_time = time.perf_counter() #perf_counter() returns time of the moment when it was called

    def stop(self):
        """Отстановить таймер и сообщить о времени вычисления"""

        if self._start_time is None:
            print("Таймер не работает. Используйте .start() для его запуска")

        elapsed_time = (time.perf_counter() - self._start_time)*1000
        print(f"\n Выполнение кода заняло {elapsed_time:0.4f} миллисекунд")
        self._start_time = None