# for progressbar
import sys


class Gauge:
    def __init__(self, name: str, max_value: int, width: int = 20):
        self.name = name
        self.value = 0
        self.max_value = max_value
        self.width = width if width < 25 else 25
        print(f"{name}...")

    def show_progress(self, value):
        self.value = value
        indicator_width = int(self.width * value / self.max_value)
        indicator_percent = int(100 * value / self.max_value)
        out_str = f"[{'░' * indicator_width}{' ' * (self.width - indicator_width)}] {indicator_percent}%\t({value}/{self.max_value})\r"
        if value == self.max_value:
            out_str += "\n"

        sys.stdout.write(out_str)
        sys.stdout.flush()
